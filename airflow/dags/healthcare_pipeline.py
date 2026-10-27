"""Run the verified project stages serially; no cloud or file I/O at DAG import."""
from datetime import datetime,timedelta,timezone
import hashlib
import os
import subprocess
from airflow.sdk import dag,task,get_current_context


@dag(dag_id='healthcare_cloud_pipeline',schedule=None,start_date=datetime(2026,1,1,tzinfo=timezone.utc),
     catchup=False,max_active_runs=1,tags=['healthcare','portfolio','data-quality'],
     default_args={'owner':'bishal','retry_delay':timedelta(seconds=30),'execution_timeout':timedelta(minutes=15)})
def healthcare_pipeline():
    @task
    def run_stage(step):
        context=get_current_context()
        run_id=context['run_id']
        run_key='af_'+hashlib.sha256(run_id.encode()).hexdigest()[:24]
        subprocess.run([os.environ.get('PIPELINE_PYTHON','/opt/pipeline-venv/bin/python'),'-m','pipeline.orchestrate',
            '--step',step,'--run-key',run_key,'--dag-run-id',run_id],
            cwd=os.environ.get('PIPELINE_PROJECT_ROOT','/opt/project'),check=True)
        return run_key

    previous=None
    for name,retries in [('validate_source',0),('export_snapshot',1),('publish_s3',2),
                         ('load_snowflake',1),('dbt_build',0),('verify_reports',0),('finalize',0)]:
        current=run_stage.override(task_id=name,retries=retries)(name)
        if previous is not None:previous >> current
        previous=current


healthcare_pipeline()
