# Project handoff and cloud completion plan

Prepared October 26, 2026 for Bishal Sunchury.

## Decisions carried into this workspace

- Build a new healthcare pipeline from scratch, with a first cloud release targeted for tomorrow, October 27.
- Project title proposed in the previous discussion: **Healthcare Data Pipeline & Automated Quality Validation**.
- Context: a friend can refer Bishal for Abacus Insights Data Engineer and QA Data Engineer openings. The healthcare domain is known; no job description or responsibilities have been supplied. Do not claim the company uses this stack.
- Proposed stack: Synthea CSV → PostgreSQL source simulator → Python → AWS S3 → Snowflake → dbt, orchestrated by Airflow.
- First three entities: patients, encounters, payers. Use synthetic records.
- Inspectability: setup/architecture README, public GitHub source when ready, actual execution evidence, reconciliation, quality tests, duplicate-free reruns, failure/correction demonstration, and reporting SQL by month, encounter type, and payer.
- Use the Data Engineer resume base for either referral, emphasizing data validation and failure investigation for QA Data Engineer. Final role-specific tailoring waits for a real description or better responsibilities.

The earlier retail pipeline remains a separate project. No retail counts, dates, or performance measurements are attributed to this healthcare rebuild. The broader retail white paper and PL-300 work do not expand this first cloud-release scope.

## Business question

Can we produce a trustworthy encounter dataset for monthly utilization and cost reporting while detecting source defects and preventing duplicate rows during reruns?

The Data Engineer story is source ingestion, batch design, relational modeling, reliable loads, and orchestration. The QA Data Engineer story is explicit contracts, reconciliation, defect injection, failure diagnosis, rollback, and reproducible recovery. These are two views of the same implementation.

## Model contract

| Intended model | Grain | Key | Required relationships |
| --- | --- | --- | --- |
| `dim_patient` | One synthetic patient | Synthea patient UUID | None |
| `dim_payer` | One payer, including the source's no-insurance payer if present | Synthea payer UUID | None |
| `fct_encounter` | One encounter, not one claim line item | Synthea encounter UUID | Patient UUID and payer UUID |
| `mart_monthly_utilization` | UTC encounter start month × encounter class × payer UUID | Composite of group keys | Fact to dimensions |

Type intent for Snowflake: IDs and codes as VARCHAR, dates as DATE, encounter times as TIMESTAMP_TZ normalized to UTC, monetary values as integer cents or NUMBER(18,2) after explicit cent conversion. Never store money in approximate FLOAT. Retain source batch ID, file checksum, and loaded-at timestamp as lineage. Monetary reconciliation must compare compatible units.

The current local source tables are a deliberately small projected source model, not these final dbt warehouse models. `code` is kept as text to preserve source semantics. A corrected patient/payer should update a Type 1 dimension for this release; historical Type 2 dimensions are deferred.

## Milestones and current state

| Order | Deliverable | State at this handoff | Exit evidence |
| --- | --- | --- | --- |
| 1 | Official data, source contract, quality/replay proof | Executed locally using SQLite; 15 tests pass | Source provenance, five scenario reports, summary JSON |
| 2 | PostgreSQL source | Verified by Bishal on Windows; full rerun counts reconcile with zero changes | First load and PostgreSQL rerun results supplied in the conversation |
| 3 | Python extraction to AWS S3 | Verified on Windows: PostgreSQL export matches expected counts/content hash; actual S3 upload and full reuse on retry in Ohio | `evidence/cloud_handoff/user_reported_results.json` transcribes provided outputs; original local receipt remains on Bishal's machine |
| 4 | Snowflake raw load and keyed updates | Verified initial load, unchanged replay, warehouse correction/recovery, orphan rejection and forced rollback; RAW unchanged by QA proof | COPY/load result, rejected-row behavior, source-to-target reconciliation, rerun proof |
| 5 | dbt models/tests and SQL reporting | Nine models built and 37 tests passed in Snowflake; reports reconciled; docs catalog generated successfully | `dbt build` results, model documentation/lineage, sample query outputs |
| 6 | Airflow orchestration | Docker build and preflight passed; real seven-stage workflow finalizer passed; local stand-in failure blocking checked separately | Successful DAG run, retry/failure/recovery task logs |
| 7 | GitHub/portfolio handoff | README, local proof and CI definition prepared; public repo pending | Public link, reviewed logs/images, supported project bullets |

## S3 batch design

Extract all three source tables from one consistent PostgreSQL read transaction. Write normalized CSVs plus a manifest with table name, schema version, row count, SHA-256, extraction timestamp, and batch ID. Use a dedicated project prefix and a new immutable prefix per batch; write a completion manifest only after all data uploads succeed. The warehouse must read only complete batches. Retries should re-use a batch ID or deterministically address the same content.

Use AWS's normal SDK credential chain locally. Keep credentials out of code, screenshots, manifests, and git. Configure a private project bucket/prefix and use the authorized account's IAM permissions. Verified bucket: bishal-healthcare-pipeline-bisar01, Ohio/us-east-2. Actual publication and retry succeeded using a temporary account-root browser-login session. No scoped uploader role was created. Snowflake access uses a separate read-only integration role prepared under `infra/`. No cost measurement has been supplied.

## Snowflake load design

Use a dedicated demo database, schemas, and warehouse in the user's account. Upload to S3, configure a Snowflake stage with an authorized S3 storage integration, then copy the precise batch into a batch landing table. Abort a malformed load; validate required values, source duplicates, and relationships before publishing models. Load patient/payer parents before encounter children. Source-level validation is not a replacement for warehouse-level checks after transit.

Merge by stable source UUID only after each batch has at most one record per key. Use explicit source-batch attribution and a content hash so reruns update only changed content. Reconcile source manifest counts and sums plus batch-key coverage/content. Do not compare a small batch count to a cumulative table count. Demonstrate the same unchanged rerun and late-correction cases in Snowflake before claiming cloud idempotency. Do not rely solely on COPY file-history deduplication.

Official reference: https://docs.snowflake.com/en/sql-reference/sql/merge

## dbt and Airflow

dbt: define raw sources, typed staging models, patient/payer dimensions, the encounter fact, and the monthly mart. Test not-null/unique IDs, both relationships, dates, accepted encounter classes after inspecting the dataset, nonnegative costs, and source-to-fact encounter counts/amounts. Use explicit `unique_key` plus uniqueness tests if configuring incremental models. A declared `unique_key` alone does not establish data quality. For a small release, full rebuilds of marts are acceptable while raw target changes use keyed merges.

Airflow task flow: verify source/export → publish S3 batch → load Snowflake → validate/reconcile → `dbt build` → persist evidence. Fail downstream publication when upstream validation fails; use bounded retries for transient service failures. Keep side effects inside tasks, avoid calls to cloud services while parsing the DAG, and initially allow one active run. Preserve run IDs/batch IDs across task logs. Choose and pin the installed Airflow/dbt/connector versions after the user's environment is known; no untested version compatibility is implied here.

Official dbt reference: https://docs.getdbt.com/reference/resource-configs/unique_key

## First release definition of done

1. Another person can follow the README with their own configured accounts.
2. All agreed stack stages have real successful execution evidence.
3. Source counts and costs reconcile through the encounter fact.
4. An identical cloud batch rerun adds zero duplicate encounter keys.
5. A deliberately invalid source batch fails with a specific diagnosis; a corrected batch succeeds.
6. A late change updates exactly the expected existing record.
7. Three reporting views are available: monthly encounter utilization, encounter-class mix, and payer cost/coverage. Document synthetic data and metric semantics.
8. GitHub contains readable source, setup, architecture, reviewed outputs, limitations, and no secrets. A two-minute walkthrough can show the successful run, failed run, and recovery.

Defer ML predictions, a dashboard build, streaming, additional clinical tables, Spark/Databricks, and production deployment. They are not part of the latest proposed first-release stack.

## Resume and interview integrity

Already supportable: a local Python/SQL prototype processing 53,346 synthetic encounters with automated source validation, content reconciliation, duplicate-free replay, and a demonstrated rejected/corrected batch. PostgreSQL source load and duplicate-free replay were verified by Bishal on Windows. PostgreSQL export and actual S3 publication/retry were verified by Bishal. The batch counts are 1,171 patients, 10 payers and 53,346 encounters. AWS S3 publication is now supportable; Snowflake initial warehouse loading and duplicate-free replay are now supportable from user-returned run results. Isolated warehouse correction, recovery, orphan rejection and rollback are also verified. This is warehouse fault injection, not a new PostgreSQL-to-S3 correction batch. Actual Snowflake dbt execution is now verified: nine models succeeded, all 37 data tests passed, and no tasks warned/failed/skipped. The real manually triggered Airflow workflow also passed on Bishal's Docker host, with its final summary preserving all counts and cents. Actual cloud failure/recovery through Airflow remains untested; QA failure evidence is separately scoped. Do not invent throughput gains, production use, HIPAA compliance, employer data access, or prior healthcare work experience from this project.

After cloud execution, draft resume bullets from the final verified counts, architecture, checks and failure/recovery evidence. For the referral, send the Data Engineer resume, public repository and brief project summary once ready. No message has been sent and no repository has been published by this handoff.

## Immediate next action

Python 3.12 in Anaconda, WSL, Docker Desktop, PostgreSQL load and duplicate-free PostgreSQL replay are verified on Bishal's Windows machine. PostgreSQL export and actual AWS S3 publication/retry are now verified. Snowflake account UJ02641 is in AWS_US_EAST_2, with ACCOUNTADMIN available. Snowflake integration properties and a successful staged manifest result have now been returned by Bishal. Bishal has now returned successful initial-load and duplicate-free replay results from `sql/03_snowflake_load.sql`. The isolated warehouse proof also passed all four cases with RAW unchanged. The dbt connection and nine-model, 37-test Snowflake build have now passed. The user also returned the exact reporting summary and a payer sample that independently reconciled. Bishal has now built the Airflow image, passed preflight and returned a successful final summary from the real seven-task workflow. dbt docs generation also passed. Preserve the original execution artifacts, publish the reviewed repository, inspect hosted CI results, and use the supported project bullets and walkthrough in `docs/PORTFOLIO.md`. See `docs/SNOWFLAKE_LOAD.md`. No credentials should be sent in chat. Follow `docs/EXPORT_AND_S3.md`.
