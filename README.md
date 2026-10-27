# Healthcare Data Pipeline & Automated Quality Validation

A healthcare data engineering portfolio project by **Bishal Sunchury**. It turns a historical Synthea snapshot into validated encounter analytics, with repeatable batch loads, explicit failure checks, and an Airflow workflow connecting PostgreSQL, AWS S3, Snowflake, and dbt.

**Verified execution:** the real seven-task Airflow workflow passed in local Docker with AWS and Snowflake connections. Nine dbt models succeeded, all 37 dbt tests passed, and source/warehouse/reporting totals reconciled. dbt documentation generation also passed. GitHub-hosted CI is configured; its hosted results remain pending.

## Verified results

| Metric | Result |
| --- | ---: |
| Patients, source / warehouse / analytics | 1,171 |
| Payers, source / warehouse / analytics | 10 |
| Encounters, source / warehouse / fact | 53,346 |
| Total claim cost | 686,831,295 cents ($6,868,312.95) |
| Payer coverage | 330,739,157 cents ($3,307,391.57) |
| Monthly utilization groups | 15,827 |
| dbt models / data tests | 9 succeeded / 37 passed |

The Airflow run key is `af_a92b2cb0f5a4ad248ea66f76`. Its content hash matches the original verified snapshot. The returned finalizer summary is [here](evidence/cloud_handoff/airflow_finalize_user_returned.json), with [provenance and execution limits](evidence/cloud_handoff/airflow_execution.md). The original returned dbt build log is [here](evidence/cloud_handoff/dbt_build_windows.txt). [Documentation generation evidence](evidence/cloud_handoff/dbt_docs_generation.json) records the subsequent successful catalog build.

## Architecture

```mermaid
flowchart TD
    A["Historical Synthea CSVs"] --> B["Validate and upsert PostgreSQL source"]
    B --> C["Python consistent snapshot export"]
    C --> D["Private S3 batch and completion manifest"]
    D --> E["Verify remote files and merge Snowflake RAW"]
    E --> F["dbt dimensions, fact and reports"]
    F --> G["Reconcile and finalize run evidence"]
    O["Airflow: seven ordered tasks"] -.-> B
    O -.-> E
    O -.-> F
    O -.-> G
```

PostgreSQL simulates a source system using projected fields from three CSVs. Python extracts all three tables in one consistent read transaction. Each S3 batch contains three normalized CSVs and a manifest published last. The Snowflake loader validates the batch before keyed merges in a transaction. dbt builds typed staging views, Type 1 patient/payer dimensions, an encounter fact, and three reporting views.

Airflow runs `validate_source → export_snapshot → publish_s3 → load_snowflake → dbt_build → verify_reports → finalize`. It is manually triggered, permits one active run, and retains a stable batch key during retries. A new run receives a new batch prefix. The finalizer requires successful evidence for every preceding task from that same run.

## Reliability and quality checks

- Source contracts check required headers/values, UUIDs, duplicate keys, date/timestamp chronology, exact nonnegative integer cents, and both encounter foreign keys before loading.
- Export verification checks file bytes, SHA-256, normalized row hashes, counts, keys, relationships, and monetary totals. The automated loader independently downloads and verifies the S3 batch before Snowflake loading.
- Conditional S3 publication reuses identical objects on retry and rejects conflicts. It publishes the completion manifest only after the data files are ready.
- Snowflake merges by source UUID, updates changed content, and reconciles loaded keys/values before committing. Actual initial load and unchanged replay passed.
- dbt tests cover IDs, dimension relationships, accepted encounter classes, chronology, exact RAW-to-fact content and cents, reporting grains, and report reconciliation.

The first release uses **full-snapshot extraction with incremental target changes**. Missing source keys are retained. It does not implement CDC, source deletions, or historical Type 2 dimensions.

## Failure and recovery evidence

| Demonstration | Observed result | Execution scope |
| --- | --- | --- |
| Identical snapshot replay | Zero inserts/updates; no duplicate keys | SQLite, PostgreSQL and Snowflake verified separately |
| Invalid encounter patient | Rejected before source loading; prior target preserved | Local source proof |
| Late encounter correction | Exactly one existing row updated by $1.00 | Local proof and isolated Snowflake QA copies |
| Recovery | Original content restored | Local proof and isolated Snowflake QA copies |
| Orphan rejection / forced transaction failure | Target values preserved; four warehouse QA cases passed overall | Isolated Snowflake QA copies; RAW unchanged |
| Airflow source-task failure | Six downstream tasks became upstream_failed | Actual Airflow engine with local stand-in stage commands |

The successful cloud Airflow run is real. The Airflow failure test used stand-in commands; it is not a failed cloud run. The warehouse correction tests do not imply a corrected PostgreSQL-to-S3 batch was published. See [warehouse QA](docs/SNOWFLAKE_QUALITY_PROOF.md), [local reviewed results](evidence/reviewed_current/summary.json), and [Airflow local validation](evidence/airflow_local/summary.json).

## Run locally

Use **Python 3.12**. From a GitHub clone, download the official sample before running the pipeline:

```bash
python scripts/download_data.py
python -m pip install cryptography==50.0.2
python -m unittest discover -s tests -v
python -m pipeline.run
python -m pipeline.run
```

The downloader checks the archive digest against [recorded provenance](data/source/provenance.json). The project ZIP also includes the three original CSVs for an offline first run; Git ignores these CSVs.

The SQLite source path uses the standard library. The optional cryptography installation enables three credential-helper tests, giving 43 Python tests in total; without it, those three tests are skipped. These tests use handmade fixtures and fake AWS clients, with no cloud credentials required. They do not replace the real cloud/dbt evidence.

The second pipeline run should report zero inserted and zero updated rows. Each run writes a new evidence directory. To repeat the five full-sample local scenarios:

```bash
python scripts/prove_local.py
```

This creates fresh evidence rather than overwriting reviewed results. A prior Windows cleanup error was fixed by explicitly closing SQLite verification connections; regression tests check handle closure.

## Configure the cloud workflow

These setup documents describe the demonstrated account. **Use your own AWS account/bucket/roles and Snowflake account/integration identifiers** when reproducing it elsewhere; review the SQL and `infra/` configuration before running. The source database is a dedicated demo instance.

| Stage | Instructions |
| --- | --- |
| PostgreSQL source | Copy `.env.example` to `.env`, choose a password, then `docker compose up -d --wait`; install `requirements-postgres.txt` |
| Consistent export and S3 publication | [Export and S3 setup](docs/EXPORT_AND_S3.md) |
| Read-only Snowflake S3 integration | [Infrastructure setup](infra/README.md) |
| Snowflake typed loading and replay | [Snowflake loader](docs/SNOWFLAKE_LOAD.md) |
| Isolated warehouse fault injection | [Warehouse QA proof](docs/SNOWFLAKE_QUALITY_PROOF.md) |
| dbt key-pair user, models, tests and docs | [dbt setup](docs/DBT_SETUP.md) |
| Airflow image, local runtime, preflight and trigger | [Airflow setup](docs/AIRFLOW_SETUP.md) |

For manual PostgreSQL commands, set `DATABASE_URL` in the current terminal using `.env` credentials and host port **15432**. Compose reads `.env`; Python reads the terminal environment. URL-encode password characters where necessary. Anaconda CMD example:

```bat
set "DATABASE_URL=postgresql://healthcare:YOUR_URL_ENCODED_PASSWORD@127.0.0.1:15432/healthcare"
python -m pipeline.run --engine postgres
python -m pipeline.export --engine postgres
```

The verified environment used Windows/Anaconda Python 3.12, Docker Desktop/WSL, PostgreSQL 16, Airflow 3.3.2, dbt-core 1.12.6, and dbt-snowflake 1.12.4. Dependencies are isolated from Airflow inside the Docker image. This release uses a temporary AWS browser-login profile, not a persistent unattended uploader identity.

## Analytics and metric semantics

The nine models are three staging views, `dim_patient`, `dim_payer`, `fct_encounter`, and three reporting views:

| Report | Grain |
| --- | --- |
| `mart_monthly_utilization` | UTC start month × encounter class × payer UUID |
| `mart_encounter_class_mix` | UTC start month × encounter class |
| `mart_payer_cost_coverage` | UTC start month × payer UUID |

[Reporting SQL](sql/05_reporting_queries.sql) provides inspection queries. The fact grain is one encounter, not one claim line. Claim cost is a synthetic encounter-level amount, not revenue or adjudicated claims. Money reconciles in integer cents; displayed currency divides by 100. Distinct patient counts are not additive across groups. Payer IDs are join keys; names are labels.

The historical April 2020 archive has no current-time freshness threshold. Its legacy payer header lacks optional `Ownership`; the source validator records an explicit blank default for this known adaptation.

Generate dbt documentation using:

```bat
python scripts\run_dbt.py docs-generate
```

Generated catalog/manifest/site artifacts stay in the ignored `dbt/target` directory. Private `.env`, `local/`, generated dbt profiles, key files, databases and unreviewed runtime evidence are also ignored. `.dockerignore` restricts the image build context to the required dependency files and Dockerfile. Keep your generated runtime files private.

## Scope and limitations

This is a portfolio implementation with synthetic historical records, local Docker orchestration, and real AWS/Snowflake execution. It has not established production availability, performance benchmarks, managed secrets, persistent unattended AWS authentication, healthcare compliance, CDC, or production monitoring. No model accuracy or business outcome is claimed. GitHub-hosted CI results should be inspected after publication; its local test definition alone is not a hosted pass.

A [portfolio walkthrough and supported project bullets](docs/PORTFOLIO.md) explain how to present the work. Detailed implementation decisions remain in [the project plan](docs/PROJECT_PLAN.md).

## Data sources

- [Synthea project](https://github.com/synthetichealth/synthea)
- [Official sample repository](https://github.com/synthetichealth/synthea-sample-data)
- [CSV data dictionary](https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary)
- [Exact archive URL, digest and file hashes](data/source/provenance.json)

All records in this project are synthetic. Source project licensing remains with its respective authors.
