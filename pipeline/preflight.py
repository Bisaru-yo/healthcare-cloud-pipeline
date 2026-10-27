"""Check mounted runtime inputs and external connections before triggering a DAG."""
from contextlib import closing
import json
import os
from pathlib import Path
from .orchestrate import load_runtime,s3_client
from .snowflake_load import connect_snowflake,PROJECT_BUCKET
from .storage import connect


def main():
    checks={}
    try:
        config=load_runtime(os.environ.get('PIPELINE_RUNTIME_CONFIG','local/airflow_runtime.json'))
        checks['mounted_key']='passed'
        with closing(connect(config['database_url'],'postgres')) as connection:
            with connection.cursor() as cursor:cursor.execute('SELECT 1');assert cursor.fetchone()[0]==1
        checks['source_postgres']='passed'
        client=s3_client(config)
        client.list_objects_v2(Bucket=PROJECT_BUCKET,Prefix='healthcare/',MaxKeys=1)
        checks['s3_profile']='passed'
        for role in ('HEALTHCARE_LOADER','HEALTHCARE_TRANSFORMER'):
            with closing(connect_snowflake(config,role)) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute('SELECT CURRENT_ROLE()');assert cursor.fetchone()[0]==role
            checks[role]='passed'
        print(json.dumps({'status':'passed','checks':checks}))
        return 0
    except Exception as exc:
        print(json.dumps({'status':'failed','checks_completed':checks,'error_type':type(exc).__name__}))
        return 1


if __name__=='__main__':raise SystemExit(main())
