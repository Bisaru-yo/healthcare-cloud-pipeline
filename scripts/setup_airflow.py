"""Prepare private local Docker settings; reuse the working Snowflake key/login."""
import argparse
import getpass
import json
import os
from pathlib import Path
import secrets
import subprocess
from urllib.parse import quote
from cryptography.hazmat.primitives import serialization
from dotenv import dotenv_values

ROOT=Path(__file__).resolve().parents[1]


def private_write(path,payload):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
    with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as handle:handle.write(payload)


def prepare(project,key_path,passphrase,aws_home):
    project,key_path,aws_home=Path(project),Path(key_path),Path(aws_home)
    serialization.load_pem_private_key(key_path.read_bytes(),password=passphrase.encode())
    settings=dotenv_values(project/'.env',interpolate=False)
    password=settings.get('POSTGRES_PASSWORD')
    if not password or password=='replace_with_a_local_password':raise ValueError('POSTGRES_PASSWORD must be configured')
    if not (aws_home/'config').is_file():raise FileNotFoundError('AWS configuration folder is missing')
    local=project/'local';local.mkdir(exist_ok=True)
    old=dotenv_values(local/'airflow.env',interpolate=False) if (local/'airflow.env').exists() else {}
    values={'AIRFLOW_DB_PASSWORD':old.get('AIRFLOW_DB_PASSWORD') or secrets.token_urlsafe(24),
            'AIRFLOW_JWT_SECRET':old.get('AIRFLOW_JWT_SECRET') or secrets.token_urlsafe(48),
            'AIRFLOW_UI_PORT':old.get('AIRFLOW_UI_PORT') or '18080',
            'AWS_HOME':aws_home.resolve().as_posix(),'SNOWFLAKE_KEY_FILE':key_path.resolve().as_posix()}
    if any("'" in value or '\n' in value for value in values.values()):raise ValueError('Unsupported quote/newline in a mount path')
    private_write(local/'airflow.env',''.join(k+"='"+v+"'\n" for k,v in values.items()))
    runtime={'database_url':'postgresql://healthcare:'+quote(password,safe='')+'@postgres:5432/healthcare',
             'snowflake_account':'pbsfajr-lw57115','private_key_path':'/run/project/snowflake_dbt_key.p8',
             'key_passphrase':passphrase,'aws_profile':'healthcare-cloud','aws_region':'us-east-2'}
    private_write(local/'airflow_runtime.json',json.dumps(runtime,indent=2)+'\n')
    ui=local/'airflow_passwords.json'
    if not ui.exists():private_write(ui,json.dumps({'bishal':secrets.token_urlsafe(18)})+'\n')
    return {'status':'prepared','ui_url':'http://localhost:'+values['AIRFLOW_UI_PORT'],
            'ui_user':'bishal','ui_password_file':str(ui),'runtime_config':str(local/'airflow_runtime.json')}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--aws-home',default=str(Path.home()/'.aws'))
    args=parser.parse_args()
    try:
        config=json.loads((ROOT/'local/dbt_key_location.json').read_text())
        result=subprocess.run(['aws','sts','get-caller-identity','--profile','healthcare-cloud',
            '--query','Account','--output','text','--no-cli-pager'],capture_output=True,text=True,timeout=45,check=True)
        if result.stdout.strip()!='009285528415':raise ValueError('AWS account differs from this project')
        secret=os.environ.get('DBT_ENV_SECRET_SNOWFLAKE_KEY_PASSPHRASE') or getpass.getpass('Existing Snowflake key passphrase: ')
        print(json.dumps(prepare(ROOT,config['private_key_path'],secret,args.aws_home)))
    except subprocess.CalledProcessError:
        raise SystemExit('AWS profile check failed. Run aws login --profile healthcare-cloud, then retry.') from None
    except Exception as exc:
        raise SystemExit('Airflow preparation failed ('+type(exc).__name__+'). Check local .env, key passphrase and AWS profile paths.') from None


if __name__=='__main__':main()
