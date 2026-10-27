"""Run one dbt command with a privately entered key passphrase and save its outcome."""
import argparse
import getpass
import json
import os
from pathlib import Path
import shutil
from collections import Counter
from datetime import datetime, timezone
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
SECRET_ENV = 'DBT_ENV_SECRET_SNOWFLAKE_KEY_PASSPHRASE'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['debug','parse','build','docs-generate'])
    args = parser.parse_args()
    config = ROOT/'local/dbt_key_location.json'
    if not config.is_file() or not (ROOT/'dbt/profiles.yml').is_file():
        parser.error('Run python scripts/setup_snowflake_dbt.py first.')
    key_path = Path(json.loads(config.read_text())['private_key_path'])
    if not key_path.is_file():
        parser.error('The configured local key is missing.')
    os.environ['SNOWFLAKE_PRIVATE_KEY_PATH'] = key_path.as_posix()
    inherited_secret = os.environ.get(SECRET_ENV)
    if not inherited_secret:
        os.environ[SECRET_ENV] = getpass.getpass('Key passphrase: ')
    os.environ['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
    started = datetime.now(timezone.utc)
    folder = ROOT/'evidence'/('dbt_'+started.strftime('%Y%m%dT%H%M%S')+'_'+uuid4().hex[:8])
    folder.mkdir(parents=True)
    command = ['docs','generate'] if args.command == 'docs-generate' else [args.command]
    try:
        from dbt.cli.main import dbtRunner
        result = dbtRunner().invoke(command+['--project-dir',str(ROOT/'dbt'),'--profiles-dir',str(ROOT/'dbt')])
        success = bool(result.success)
        record = {'command':args.command,'status':'passed' if success else 'failed',
                  'started_at':started.isoformat(),'finished_at':datetime.now(timezone.utc).isoformat(),
                  'error_type':type(result.exception).__name__ if result.exception else None}
        results = ROOT/'dbt/target/run_results.json'
        # Do not attach a prior run's result if this invocation failed before writing.
        if args.command == 'build' and results.is_file() and results.stat().st_mtime >= started.timestamp():
            raw = json.loads(results.read_text())
            statuses = Counter(item['status'] for item in raw.get('results',[]))
            record['result_status_counts'] = dict(statuses)
            record['invocation_id'] = raw.get('metadata',{}).get('invocation_id')
            shutil.copyfile(results,folder/'run_results.json')
        (folder/'summary.json').write_text(json.dumps(record,indent=2)+'\n')
        print(json.dumps({'status':record['status'],'command':args.command,
                          'result_status_counts':record.get('result_status_counts'),
                          'evidence':str(folder/'summary.json')}))
        return 0 if success else 1
    finally:
        if not inherited_secret:
            os.environ.pop(SECRET_ENV,None)


if __name__ == '__main__':
    raise SystemExit(main())
