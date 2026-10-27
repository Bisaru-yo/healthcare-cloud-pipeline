# Airflow local orchestration

## Starting evidence

Bishal verified the full manual PostgreSQL -> S3 -> Snowflake -> dbt path. The real dbt build passed nine models and 37 tests. The returned encounter fact summary reconciles 53346 encounters, 686831295 total claim cost cents and 330739157 payer coverage cents. A returned 20-row payer report also matched an independent aggregation of the normalized source. Bishal has now built the Docker image, passed container preflight and returned a passed final result for the real seven-stage workflow. See `evidence/cloud_handoff/airflow_execution.md` and the accompanying returned JSON for the run ID, results and evidence limits.

## Design

Airflow 3.3.2 runs in a Docker Desktop Linux container with the local-development `standalone` command and LocalExecutor. A separate PostgreSQL service stores Airflow metadata. The existing `postgres` source service and healthcare_pg volume remain defined by compose.yaml. The extra compose file adds only the Airflow services/volumes. The UI binds to 127.0.0.1:18080 by default. This is a local portfolio deployment with SimpleAuthManager, not a production deployment.

The DAG `healthcare_cloud_pipeline` has seven ordered tasks:

1. validate_source: validate the synthetic CSVs and upsert the PostgreSQL source simulator.
2. export_snapshot: extract one consistent snapshot and independently verify its CSVs/manifest.
3. publish_s3: upload the three exact CSVs and publish the completion manifest last.
4. load_snowflake: download and verify S3 bytes, row hashes, keys, relationships and sums, then execute the reviewed typed loader and atomic merges using HEALTHCARE_LOADER.
5. dbt_build: build the nine models and require all 37 tests to pass.
6. verify_reports: compare dimensions/fact to cumulative RAW tables and reconcile reporting totals in integer cents.
7. finalize: require successful evidence from every prior task and save a consolidated result.

The DAG has no scheduled interval yet (`schedule=None`), no catchup and at most one active run. Triggering it manually starts the entire workflow automatically. No file/credential reads or cloud calls occur during DAG import. Do not edit the source snapshot or run an independent writer while the demo run is executing.

The run key is `af_` plus 24 hexadecimal characters derived from the Airflow run ID. Task retries retain that key. Export retries reuse a previously verified immutable local batch; S3 retries reuse matching objects; Snowflake retries reconcile stable keys. Bounded retries are configured for extraction, publication and loading. Validation/dbt/reconciliation failures stop downstream tasks. A new DAG run gets a new S3 batch prefix, even if the data content is identical. This first DAG does not propagate source deletions or implement CDC.

Airflow and the pipeline use separate Python environments inside the image. Airflow comes from the pinned official apache/airflow:3.3.2-python3.12 image; the isolated /opt/pipeline-venv installs the project's dbt, PostgreSQL and AWS dependencies. The DAG launches one pipeline CLI step per task. This avoids combining Airflow's dependency constraints with dbt's dependencies.

## Prepare Windows

Docker Desktop/WSL and the existing source PostgreSQL service have already been verified. Apache's Docker quickstart recommends at least 4 GB of Docker memory, ideally 8 GB. The image build pulls the Airflow image and installs dependencies, so the first build can take several minutes.

Copy the updated ZIP's inner project contents into the existing project folder, merging folders and allowing code replacements. Keep the same healthcare-cloud Anaconda environment.

Open `sql/06_airflow_loader_role.sql`, copy the entire file into the existing Snowflake SQL editor and run all with Ctrl+Shift+Enter. It grants a separate HEALTHCARE_LOADER role to the existing HEALTHCARE_DBT_USER. The role can read/insert/update the three RAW tables, append/read the audit, use the stage/JSON format and create the loader's temporary objects. No ACCOUNTADMIN role or S3 writes are granted to that service user. dbt continues using HEALTHCARE_TRANSFORMER.

From the project directory:

```bat
python scripts\setup_airflow.py
```

Enter the existing Snowflake key passphrase. The helper checks the current AWS profile's account, validates the encrypted key, reads the local source password, and creates private runtime files under ignored local/:

- airflow.env: mount paths and stable generated Airflow metadata/JWT settings.
- airflow_runtime.json: source connection and the existing key passphrase for unattended tasks.
- airflow_passwords.json: a generated local Airflow UI password for user bishal.

These private local files are excluded from git, the project ZIP and the Docker build context. The existing private key is mounted read-only from the user's home directory. Runtime credentials are a local development file arrangement, not a managed production secret store. The standard AWS profile/cache directory is mounted so Boto3 with CRT can use and refresh the same browser-login session. The existing temporary account-root AWS session is reused; no scoped uploader role is claimed. When its login session expires, renew it on Windows with `aws login --profile healthcare-cloud` before running the DAG again. This demo does not establish persistent unattended AWS identity.

If setup reports an AWS profile error:

```bat
aws login --profile healthcare-cloud
python scripts\setup_airflow.py
```

The helper preserves existing Airflow database/JWT/UI settings on reruns, so it does not accidentally change a running metadata database password.

## Build and start

Use the provided wrapper so Compose always includes both files and both environment files:

```bat
scripts\airflow.cmd up -d --build --wait
scripts\airflow.cmd ps
```

Expect the existing postgres source, airflow-meta and airflow services to be healthy. Bishal successfully built this image and returned all three services healthy. The assistant did not run Docker on his behalf.

Then test the mounted key, source database, AWS profile and both Snowflake roles:

```bat
scripts\airflow.cmd exec airflow /opt/pipeline-venv/bin/python -m pipeline.preflight
```

Expect status passed. If this fails, preserve the error type and completed checks. Actual SQL/AWS error codes and Snowflake query IDs are saved in failed task evidence when available; connection strings and private exception text are not written into public step summaries.

## Open and trigger

Open http://localhost:18080. Username: bishal. Read the generated password locally:

```bat
type local\airflow_passwords.json
```

Use its password to sign in; do not paste the password into chat or public evidence. In the Airflow Dags list, find healthcare_cloud_pipeline, unpause it and trigger one run. Watch the seven tasks progress. A green successful DAG is evidence only after all real task stages pass; startup health or a successful import alone does not prove the cloud workflow ran.

Inspect the final result from the latest run:

```bat
python -c "import json; from pathlib import Path; p=max(Path('evidence/airflow').glob('*/finalize.json'),key=lambda p:p.stat().st_mtime); print(json.dumps(json.loads(p.read_text()),indent=2))"
```

Check the run key and timestamp against the run you just triggered; an earlier successful result does not prove the newest run succeeded. The result includes the manifest URI, content identity, batch counts, dbt statuses and reporting counts/totals. Per-task evidence is under evidence/airflow/<run_key>; Airflow logs/state are stored in the airflow_home and metadata volumes.

To pause the local orchestration services while retaining their state:

```bat
scripts\airflow.cmd stop airflow airflow-meta
```

The source PostgreSQL service can remain available for manual work. No volume deletion is needed for normal operation.

## Local validation already completed

- All 43 Python tests passed, including remote tampering rejection, immutable retry behavior, cross-run evidence rejection, source-quality failure blocking downstream work, required final metrics, safe SQL rendering, and closed-cursor handling.
- The real installed Snowflake connector split the rendered script into 12 statements while preserving the entire atomic scripting block. This checks client-side parsing, not Snowflake execution of the newly automated role.
- Actual Airflow 3.3.2 imported the seven-task DAG. A local `airflow dags test` with stand-in stage commands reported all seven tasks successful in order. A second run deliberately failed the source command; all six downstream task states became upstream_failed. Those stage commands made no cloud calls. See evidence/airflow_local.
- Compose YAML parsed and retains the existing source service/volume. Private configuration generation was checked with an encrypted test key, special characters in a test source password, and reruns that preserve metadata/UI credentials.
- Bishal subsequently built the Docker image and passed container preflight for the mounted key, PostgreSQL, S3 and both Snowflake roles. The real workflow finalizer returned passed for run `af_a92b2cb0f5a4ad248ea66f76`, with nine successful models, 37 passed tests and matching reporting counts/cents. This result is transcribed in `evidence/cloud_handoff`; original per-task artifacts/logs remain on his machine. A live failing cloud DAG run and production deployment have not been demonstrated.

This setup targets the user's Windows Docker Desktop host. A Linux host may need matching UID/group permissions for bind-mounted key/runtime files.

References:
- https://airflow.apache.org/docs/apache-airflow/3.3.2/start.html
- https://airflow.apache.org/docs/apache-airflow/3.3.2/howto/docker-compose/index.html
- https://airflow.apache.org/docs/apache-airflow/3.3.2/tutorial/taskflow.html
- https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/auth-manager/simple/index.html
- https://docs.aws.amazon.com/sdkref/latest/guide/feature-login-credentials.html
