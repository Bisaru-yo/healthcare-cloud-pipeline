# Airflow execution returned by the project owner

This directory's `airflow_finalize_user_returned.json` transcribes the final JSON pasted by Bishal from his Windows project folder. It is user-returned execution evidence; the assistant did not access his live cloud accounts or fetch the original file. The finalizer requires successful evidence for all six preceding stages from the same run key.

- Run key: `af_a92b2cb0f5a4ad248ea66f76`.
- DAG run: `manual__2026-10-27T04:04:24.081221+00:00`.
- Final status: passed.
- dbt: nine successful models and 37 passed tests.
- Patient rows: RAW and analytics both 1,171.
- Payer rows: RAW and analytics both 10.
- Encounter rows: RAW and fact both 53,346.
- Claim cost: 686,831,295 integer cents; payer coverage: 330,739,157 integer cents.
- Monthly utilization groups: 15,827; payer cost/coverage groups: 6,976.
- Content hash matches the original verified full snapshot.

Before triggering this run, Bishal returned a successful Docker build and healthy `postgres`, `airflow-meta`, and `airflow` services. Container preflight returned passed for the mounted key, source PostgreSQL, S3 profile, HEALTHCARE_LOADER and HEALTHCARE_TRANSFORMER roles.

The actual per-task files and Airflow task logs remain on Bishal's machine. They have not been copied into this release, and no original logs or additional run results are implied. Local Airflow stand-in tests remain separately labeled under `evidence/airflow_local`; they are not used as cloud execution evidence.

This is a manually triggered workflow in local Docker connected to real AWS and Snowflake services. It does not establish a hosted production scheduler, an unattended persistent AWS identity, or production/HIPAA compliance. The earlier four warehouse fault-injection cases ran on QA copies; an actual failing cloud Airflow run has not been demonstrated.
