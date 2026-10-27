# Portfolio presentation

## Project listing

**Healthcare Data Pipeline & Automated Quality Validation**  
Independent portfolio project | Python, SQL, PostgreSQL, AWS S3, Snowflake, dbt, Apache Airflow, Docker

## Resume bullets supported by the execution evidence

- Built a healthcare data pipeline from PostgreSQL through AWS S3 to Snowflake, orchestrating seven Airflow tasks to validate and reconcile 53,346 synthetic encounters, 1,171 patients, and 10 payers.
- Developed nine dbt models for staging, patient/payer dimensions, encounter facts, and utilization/cost reporting; passed 37 data tests and reconciled counts and financial totals from RAW tables through analytics.
- Implemented immutable batch manifests, SHA-256 integrity checks, atomic keyed merges, and duplicate-free replay; demonstrated a one-record correction, recovery, orphan rejection, and transaction rollback on isolated Snowflake QA copies.

For a QA Data Engineer application, lead with the third bullet and emphasize defect investigation, exact reconciliation, and preserved target data. For a Data Engineer application, lead with orchestration and batch design. These are project claims, not employer experience. Do not add throughput gains, production availability, HIPAA compliance, or claims-processing experience from this demo.

## Two-minute walkthrough

1. **Problem and scope (about 20 seconds).** Explain the goal: produce encounter utilization and cost reports from synthetic source records while preventing duplicates and detecting invalid data. State that the records are historical Synthea data.
2. **Architecture (about 25 seconds).** Show the README diagram. Describe PostgreSQL as the source simulator, consistent Python extraction, S3 completion manifests, verified Snowflake merges, and dbt analytics. Explain that Airflow runs in local Docker and connects to real cloud services.
3. **Successful run (about 30 seconds).** Show Airflow's successful seven-task run and open its actual `evidence/airflow/af_a92b2cb0f5a4ad248ea66f76/finalize.json`. Point to 9 successful models, 37 passed tests, matching counts, and reconciled integer-cent totals.
4. **Reliability proof (about 30 seconds).** Show the unchanged Snowflake replay and four QA case results. Explain exactly one existing encounter was corrected and restored in QA copies; orphan and forced transaction failures preserved target values. The source/warehouse tests and real cloud success are distinct from the stand-in Airflow failure test.
5. **Analytics and tradeoff (about 15 seconds).** Show one reporting query or dbt lineage. Explain the fact grain, exact cents, full-snapshot extraction, retained absent keys, and why CDC/production deployment were deferred.

Use actual saved evidence or the live run page. Do not present local stand-in logs as a cloud failure/recovery run. Avoid showing private runtime files or key material during a screen share.

## Evidence to keep locally

The public release includes reviewed transcriptions plus the original returned dbt build log. Keep the original successful Airflow run directory, its task logs, the dbt docs summary, and generated catalog/manifest privately available for inspection. Generated docs and unreviewed logs are ignored by Git; selective publication should follow review rather than copying the whole runtime directory.

## Publication checks

Before the first commit, inspect `git diff --cached --name-only`. `.env`, `local/`, `dbt/profiles.yml`, private keys, local databases, source CSVs and generated dbt artifacts should be excluded. Only the reviewed evidence directories are included. After pushing, check GitHub Actions for both Linux and Windows; record hosted results only after they actually finish.

The repository link and final application wording can be added once the repository exists. No referral message has been sent by this project workflow.
