"""One CLI step per Airflow task; small run identifiers carry between tasks."""
import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import re
import sys

from .export import export_snapshot,verify_batch
from .publish import publish_batch
from .run import run
from .snowflake_load import PROJECT_BUCKET,connect_snowflake,execute_load,verify_remote_batch

ROOT=Path(__file__).resolve().parents[1]
STEPS=('validate_source','export_snapshot','publish_s3','load_snowflake','dbt_build','verify_reports','finalize')


def load_runtime(path):
    config=json.loads(Path(path).read_text(encoding='utf-8'))
    required={'database_url','snowflake_account','private_key_path','key_passphrase','aws_profile','aws_region'}
    if not required.issubset(config) or any(not config[k] for k in required):
        raise ValueError('Incomplete local runtime configuration')
    if not Path(config['private_key_path']).is_file():
        raise FileNotFoundError('Mounted Snowflake key is missing')
    return config


def atomic_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    temporary=path.with_suffix('.partial')
    temporary.write_text(json.dumps(value,indent=2,default=str)+'\n',encoding='utf-8')
    temporary.replace(path)


def successful_step(folder,name,run_key):
    value=json.loads((folder/(name+'.json')).read_text())
    if value.get('status')!='passed' or value.get('run_key')!=run_key or value.get('step')!=name:
        raise ValueError('Prior task evidence is missing or did not pass')
    return value


def s3_client(config):
    import boto3
    from botocore.config import Config
    session=boto3.Session(profile_name=config['aws_profile'],region_name=config['aws_region'])
    return session.client('s3',config=Config(connect_timeout=10,read_timeout=60,retries={'mode':'standard','max_attempts':3}))


def report_metrics(connection):
    result={}
    with closing(connection.cursor()) as cursor:
        for entity,dimension in [('patients','DIM_PATIENT'),('payers','DIM_PAYER')]:
            cursor.execute(f'SELECT (SELECT COUNT(*) FROM HEALTHCARE.RAW.{entity}), (SELECT COUNT(*) FROM HEALTHCARE.ANALYTICS.{dimension})')
            raw,actual=map(int,cursor.fetchone())
            if raw==0 or raw!=actual:raise ValueError('Dimension reconciliation failed')
            result[entity]={'raw_rows':raw,'analytics_rows':actual}
        totals={}
        for schema,table in [('RAW','ENCOUNTERS'),('ANALYTICS','FCT_ENCOUNTER')]:
            cursor.execute(f'SELECT COUNT(*),COALESCE(SUM(total_claim_cost_cents),0),COALESCE(SUM(payer_coverage_cents),0) FROM HEALTHCARE.{schema}.{table}')
            totals[schema]=list(map(int,cursor.fetchone()))
        if totals['RAW'][0]==0 or totals['RAW']!=totals['ANALYTICS']:
            raise ValueError('Fact count or integer-cent totals differ from RAW')
        result['encounters']={'raw_rows':totals['RAW'][0],'fact_rows':totals['ANALYTICS'][0],
            'total_claim_cost_cents':totals['ANALYTICS'][1],'payer_coverage_cents':totals['ANALYTICS'][2]}
        for name in ['MART_MONTHLY_UTILIZATION','MART_PAYER_COST_COVERAGE']:
            cursor.execute(f'SELECT COUNT(*),COALESCE(SUM(encounter_count),0),COALESCE(SUM(total_claim_cost_cents),0),COALESCE(SUM(payer_coverage_cents),0) FROM HEALTHCARE.ANALYTICS.{name}')
            row=list(map(int,cursor.fetchone()))
            if row[1:]!=totals['ANALYTICS']:raise ValueError('Reporting totals do not reconcile')
            result[name.lower()]={'groups':row[0]}
    return result


def execute_step(step,run_key,config,project=ROOT,dag_run_id=None):
    project=Path(project)
    if step not in STEPS or not re.fullmatch(r'af_[0-9a-f]{24}',run_key):
        raise ValueError('Invalid orchestration step or run identifier')
    folder=project/'evidence/airflow'/run_key
    index=STEPS.index(step)
    record={'run_key':run_key,'dag_run_id':dag_run_id,'step':step,'status':'running','started_at':datetime.now(timezone.utc).isoformat()}
    try:
        if index:successful_step(folder,STEPS[index-1],run_key)
        if step=='validate_source':
            source=run(project/'data/source',config['database_url'],folder/'source_runs',engine='postgres')
            record['source_run_id']=source['run_id'];record['quality']=source['quality'];record['load']=source['load']
        elif step=='export_snapshot':
            batch=project/'data/batches'/run_key
            reused=batch.exists()
            if reused:manifest=verify_batch(batch)
            else:batch,manifest=export_snapshot(config['database_url'],'postgres',project/'data/batches',run_key)
            if manifest['batch_id']!=run_key:raise ValueError('Existing export belongs to another run')
            record.update(batch_id=run_key,content_sha256=manifest['content_sha256'],reused_local_batch=reused,
                source_counts={t:m['row_count'] for t,m in manifest['tables'].items()})
        elif step=='publish_s3':
            batch=project/'data/batches'/run_key
            expected=successful_step(folder,'export_snapshot',run_key)
            manifest=verify_batch(batch)
            if manifest['content_sha256']!=expected['content_sha256']:raise ValueError('Export content changed')
            receipt=publish_batch(batch,PROJECT_BUCKET,client=s3_client(config),region=config['aws_region'])
            record.update(manifest_uri=receipt['manifest_uri'],uploaded=sum(o['action']=='uploaded' for o in receipt['objects']),reused=sum(o['action']=='reused' for o in receipt['objects']))
        elif step=='load_snowflake':
            expected=successful_step(folder,'export_snapshot',run_key)
            manifest=verify_remote_batch(s3_client(config),run_key,expected['content_sha256'])
            with closing(connect_snowflake(config,'HEALTHCARE_LOADER')) as connection:
                record['load']=execute_load(connection,manifest)
            record['verified_s3_bytes_and_row_hashes']=True
        elif step=='dbt_build':
            from dbt.cli.main import dbtRunner
            os.environ.update(SNOWFLAKE_ACCOUNT=config['snowflake_account'],SNOWFLAKE_PRIVATE_KEY_PATH=config['private_key_path'],
                DBT_ENV_SECRET_SNOWFLAKE_KEY_PASSPHRASE=config['key_passphrase'],DBT_SEND_ANONYMOUS_USAGE_STATS='false')
            try:
                outcome=dbtRunner().invoke(['build','--project-dir',str(project/'dbt'),'--profiles-dir',str(project/'dbt'),
                    '--target-path',str(folder/'dbt_target'),'--log-path',str(folder/'dbt_logs')])
            finally:
                os.environ.pop('DBT_ENV_SECRET_SNOWFLAKE_KEY_PASSPHRASE',None)
            if not outcome.success:raise RuntimeError('dbt build or tests failed')
            results=json.loads((folder/'dbt_target/run_results.json').read_text())
            counts=dict(Counter(r['status'] for r in results['results']))
            if counts.get('success')!=9 or counts.get('pass')!=37 or sum(counts.values())!=46:
                raise ValueError('Unexpected or incomplete dbt build results')
            record.update(result_status_counts=counts,invocation_id=results['metadata']['invocation_id'])
        elif step=='verify_reports':
            with closing(connect_snowflake(config,'HEALTHCARE_TRANSFORMER')) as connection:
                record['reports']=report_metrics(connection)
        elif step=='finalize':
            records={name:successful_step(folder,name,run_key) for name in STEPS[:-1]}
            record.update(batch_id=run_key,content_sha256=records['export_snapshot']['content_sha256'],
                manifest_uri=records['publish_s3']['manifest_uri'],dbt=records['dbt_build']['result_status_counts'],
                reports=records['verify_reports']['reports'],source_counts=records['export_snapshot']['source_counts'])
        record['status']='passed'
    except Exception as exc:
        record.update(status='failed',error_type=type(exc).__name__)
        for attribute,label in [('errno','error_code'),('sqlstate','sql_state'),('sfqid','query_id')]:
            value=getattr(exc,attribute,None)
            if value is not None:record[label]=value
        raise
    finally:
        record['finished_at']=datetime.now(timezone.utc).isoformat()
        atomic_json(folder/(step+'.json'),record)
        print(json.dumps({'status':record['status'],'step':step,'run_key':run_key,'evidence':str(folder/(step+'.json'))}))
    return record


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--step',choices=STEPS,required=True)
    parser.add_argument('--run-key',required=True)
    parser.add_argument('--dag-run-id',default=None)
    parser.add_argument('--config',default=os.environ.get('PIPELINE_RUNTIME_CONFIG','local/airflow_runtime.json'))
    args=parser.parse_args()
    try:execute_step(args.step,args.run_key,load_runtime(args.config),dag_run_id=args.dag_run_id)
    except Exception as exc:
        print(json.dumps({'status':'failed','step':args.step,'error_type':type(exc).__name__}),file=sys.stderr)
        return 1
    return 0


if __name__=='__main__':raise SystemExit(main())
