"""Validate model SQL and test logic on the full sample in DuckDB.

SQL is parsed as Snowflake and transpiled for this offline engine. This is not
Snowflake execution. Install requirements-dbt-validation.txt to repeat it.
"""
import argparse
import csv
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import sqlglot
import yaml
from jinja2 import Environment, StrictUndefined
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from pipeline.quality import fingerprint, read_and_validate
from pipeline.storage import COLUMNS
ORDER=['stg_patients','stg_payers','stg_encounters','dim_patient','dim_payer','fct_encounter','mart_monthly_utilization','mart_encounter_class_mix','mart_payer_cost_coverage']
ENV=Environment(undefined=StrictUndefined)
ENV.globals.update(ref=lambda n:'analytics.'+n,source=lambda s,n:'raw.'+n)


def render(text):
    return sqlglot.transpile(ENV.from_string(text).render(),read='snowflake',write='duckdb',unsupported_level=sqlglot.ErrorLevel.RAISE)[0]


def failures(connection,query):
    return connection.execute('select count(*) from ('+render(query)+')').fetchone()[0]


def generic_checks():
    docs=yaml.safe_load((ROOT/'dbt/models/staging/sources.yml').read_text())
    resources=[('raw.'+t['name'],t) for s in docs['sources'] for t in s['tables']]
    docs=yaml.safe_load((ROOT/'dbt/models/schema.yml').read_text())
    resources += [('analytics.'+m['name'],m) for m in docs['models']]
    cases=[]
    for relation,resource in resources:
        for column in resource.get('columns',[]):
            c=column['name']
            for definition in column.get('data_tests',[]):
                kind,props=(definition,{}) if isinstance(definition,str) else next(iter(definition.items()))
                args=(props or {}).get('arguments',{})
                if kind=='not_null': q=f'select {c} from {relation} where {c} is null'
                elif kind=='unique': q=f'select {c} from {relation} where {c} is not null group by {c} having count(*)>1'
                elif kind=='nonnegative': q=f'select {c} from {relation} where {c}<0'
                elif kind=='relationships':
                    parent=ENV.from_string('{{ '+args['to']+' }}').render();field=args['field']
                    q=f'select c.{c} from {relation} c left join {parent} p on c.{c}=p.{field} where c.{c} is not null and p.{field} is null'
                elif kind=='accepted_values':
                    vals=','.join("'"+str(v).replace("'","''")+"'" for v in args['values'])
                    q=f'select {c} from {relation} where {c} not in ({vals})'
                else: raise ValueError('Unhandled test: '+kind)
                cases.append((relation+'.'+c+'.'+kind,q))
    return cases


def verify(output):
    datasets,_=read_and_validate(ROOT/'data/source')
    con=duckdb.connect(':memory:')
    con.execute("SET TimeZone='UTC'")
    con.execute('CREATE SCHEMA raw');con.execute('CREATE SCHEMA analytics')
    with tempfile.TemporaryDirectory() as working:
        for table,rows in datasets.items():
            path=Path(working)/(table+'.csv')
            with path.open('w',newline='',encoding='utf-8') as handle:
                writer=csv.writer(handle);writer.writerow(COLUMNS[table]+('row_hash',))
                for row in rows:writer.writerow([row[c] for c in COLUMNS[table]]+[fingerprint(row)])
            specs=[]
            for c in COLUMNS[table]+('row_hash',):
                quoted='"'+c+'"'
                if c in ('birthdate','deathdate'):specs.append(f"cast(nullif({quoted},'') as date) as {c}")
                elif c in ('start','stop'):specs.append(f"cast(nullif({quoted},'') as timestamptz) as {c}_at")
                elif table=='encounters' and c in ('patient','payer'):specs.append(quoted+' as '+c+'_id')
                elif c=='encounterclass':specs.append(quoted+' as encounter_class')
                elif table=='encounters' and c=='code':specs.append(quoted+' as encounter_code')
                elif c.endswith('_cents'):specs.append(f'cast({quoted} as bigint) as {c}')
                else:specs.append(quoted)
            specs += ["'offline_sample' as source_batch_id","'offline_lineage' as source_file_sha256","'offline_lineage' as source_content_sha256","timestamptz '2026-10-26 00:00:00+00' as loaded_at"]
            con.execute('CREATE TABLE raw.'+table+' AS SELECT '+','.join(specs)+" FROM read_csv(?, header=true, all_varchar=true, nullstr='__never_null__')",[str(path)])
    for name in ORDER:
        path=next((ROOT/'dbt/models').rglob(name+'.sql'))
        kind='VIEW' if name.startswith(('stg_','mart_')) else 'TABLE'
        con.execute('CREATE '+kind+' analytics.'+name+' AS '+render(path.read_text()))
    cases=generic_checks()+[(p.stem,p.read_text()) for p in sorted((ROOT/'dbt/tests').glob('*.sql'))]
    baseline={name:failures(con,sql) for name,sql in cases}
    assert not any(baseline.values()),baseline
    totals=con.execute('SELECT count(*), sum(total_claim_cost_cents), sum(payer_coverage_cents) FROM analytics.fct_encounter').fetchone()
    assert totals==(53346,686831295,330739157),totals
    groups=con.execute('SELECT count(*) FROM analytics.mart_monthly_utilization').fetchone()[0]
    assert groups==15827,groups
    key=con.execute('SELECT min(encounter_id) FROM analytics.fct_encounter').fetchone()[0]
    scenarios=[('dropped_encounter','DELETE FROM analytics.fct_encounter WHERE encounter_id=?',['raw_to_fact_content','raw_to_fact_totals']),('duplicate_encounter','INSERT INTO analytics.fct_encounter SELECT * FROM analytics.fct_encounter WHERE encounter_id=?',['analytics.fct_encounter.encounter_id.unique','raw_to_fact_totals']),('orphan_payer',"UPDATE analytics.fct_encounter SET payer_id='__absent__' WHERE encounter_id=?",['analytics.fct_encounter.payer_id.relationships','raw_to_fact_content','reporting_totals'])]
    detections={}
    for name,statement,expected in scenarios:
        con.execute('BEGIN TRANSACTION');con.execute(statement,[key])
        actual={n:failures(con,q) for n,q in cases}
        assert all(actual[n]>0 for n in expected),(name,actual)
        detections[name]={n:v for n,v in actual.items() if v}
        con.execute('ROLLBACK')
    assert not any(failures(con,q) for _,q in cases)
    report={'status':'passed','engine':'DuckDB after SQLGlot transpilation from Snowflake SQL','cloud_execution':False,'models_exercised':len(ORDER),'data_checks_exercised':len(cases),'fact_rows':totals[0],'total_claim_cost_cents':totals[1],'payer_coverage_cents':totals[2],'monthly_report_groups':groups,'injected_defects_detected':detections,'post_rollback_checks':'all passed','completed_at':datetime.now(timezone.utc).isoformat()}
    output=Path(output);output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(report,indent=2)+'\n')
    con.close();print(json.dumps(report))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',default=str(ROOT/'evidence/dbt_local/summary.json'))
    verify(parser.parse_args().output)
