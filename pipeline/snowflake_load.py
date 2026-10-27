"""Verify S3 batch bytes, then render and execute the reviewed atomic loader."""
import io
import re
import tempfile
from contextlib import closing
from pathlib import Path
from .export import verify_batch

ROOT = Path(__file__).resolve().parents[1]
FIRST_BATCH = '20261026T235514_a3bfed17dc09'
FIRST_CONTENT = 'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c'
PROJECT_BUCKET = 'bishal-healthcare-pipeline-bisar01'


def validate_identity(batch_id, content_sha256):
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}',batch_id):
        raise ValueError('Unsafe batch identifier')
    if not re.fullmatch(r'[0-9a-f]{64}',content_sha256):
        raise ValueError('Invalid content identity')


def verify_remote_batch(client, batch_id, content_sha256, bucket=PROJECT_BUCKET):
    validate_identity(batch_id,content_sha256)
    with tempfile.TemporaryDirectory() as working:
        root=Path(working)
        for filename in ('manifest.json','patients.csv','payers.csv','encounters.csv'):
            response=client.get_object(Bucket=bucket,Key='healthcare/'+batch_id+'/'+filename)
            limit=1024*1024 if filename=='manifest.json' else 256*1024*1024
            with closing(response['Body']) as body:
                payload=body.read(limit+1)
            if len(payload)>limit:
                raise ValueError('S3 object exceeds the supported demo batch size')
            (root/filename).write_bytes(payload)
        manifest=verify_batch(root)
    if manifest['batch_id']!=batch_id or manifest['content_sha256']!=content_sha256:
        raise ValueError('S3 batch identity differs from the published batch')
    return manifest


def render_load_sql(manifest, template=None):
    batch_id, digest=manifest['batch_id'],manifest['content_sha256']
    validate_identity(batch_id,digest)
    text=template if template is not None else (ROOT/'sql/03_snowflake_load.sql').read_text()
    if 'USE ROLE ACCOUNTADMIN;' not in text or FIRST_BATCH not in text or FIRST_CONTENT not in text:
        raise ValueError('Reviewed SQL template changed; inspect its rendering contract')
    return text.replace('USE ROLE ACCOUNTADMIN;','USE ROLE HEALTHCARE_LOADER;').replace(FIRST_BATCH,batch_id).replace(FIRST_CONTENT,digest)


def validate_load_metrics(rows, manifest):
    by_table={r['table_name']:r for r in rows}
    if len(rows)!=3 or set(by_table)!=set(manifest['tables']):
        raise ValueError('Missing or duplicate Snowflake load metrics')
    for table,metadata in manifest['tables'].items():
        row=by_table[table]
        if row['status']!='passed' or row['source_rows']!=metadata['row_count'] or row['reconciled_rows']!=metadata['row_count']:
            raise ValueError('Snowflake batch-key reconciliation failed')
        if row['inserted']+row['updated']+row['unchanged']!=row['source_rows']:
            raise ValueError('Snowflake change counts do not reconcile')
    return rows


def execute_load(connection, manifest):
    rows=None
    for cursor in connection.execute_stream(io.StringIO(render_load_sql(manifest))):
        try:
            if cursor.description:
                names=[c[0].lower() for c in cursor.description]
                if {'table_name','reconciled_rows','status'}.issubset(names):
                    rows=[dict(zip(names,record)) for record in cursor.fetchall()]
                    for row in rows:
                        for field in ('inserted','updated','unchanged','source_rows','reconciled_rows','target_total_rows'):
                            row[field]=int(row[field])
        finally:
            cursor.close()
    if rows is None:
        raise ValueError('The loader returned no final reconciliation result')
    return validate_load_metrics(rows,manifest)


def connect_snowflake(config,role):
    import snowflake.connector
    return snowflake.connector.connect(account=config['snowflake_account'],user='HEALTHCARE_DBT_USER',
        role=role,warehouse='HEALTHCARE_WH',database='HEALTHCARE',schema='RAW' if role=='HEALTHCARE_LOADER' else 'ANALYTICS',
        authenticator='SNOWFLAKE_JWT',private_key_file=config['private_key_path'],
        private_key_file_pwd=config['key_passphrase'],login_timeout=30,network_timeout=120,
        session_parameters={'TIMEZONE':'UTC','QUERY_TAG':'healthcare_portfolio_airflow'})
