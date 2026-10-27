# First Snowflake batch load

## Verified starting point

Bishal successfully queried the completion manifest through the Snowflake S3 stage. The user-provided row matches batch `20261026T235514_a3bfed17dc09`, status `complete`, content SHA-256 `a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c`, and counts of 1171 patients, 10 payers and 53346 encounters. This proves stage access and manifest parsing; it is not yet evidence that the CSVs have loaded into warehouse tables.

## Run

Open `sql/03_snowflake_load.sql` and paste the whole file into the existing Snowflake workspace SQL editor. On Windows press Ctrl+Shift+Enter to run all statements. Run serially, with one loader at a time, using ACCOUNTADMIN for this initial demo setup. The script is pinned to this first batch and its expected content identity; it is not yet a general batch ingestion CLI.

The script creates `HEALTHCARE.RAW.PATIENTS`, `PAYERS`, `ENCOUNTERS`, and `LOAD_RUNS` if absent. It does not replace existing persistent target tables. It loads the three exact CSV filenames into new session-local landing tables using COPY with ABORT_STATEMENT. FORCE applies only to those fresh landing tables, ensuring that the second run actually exercises the merge rather than relying on COPY file-history suppression.

The gates reject incomplete or unexpected manifests, count mismatches, empty tables, duplicate keys, malformed UUIDs/hashes, missing fields, invalid dates/timestamps, reversed chronology, absent patient/payer references, malformed or out-of-range integer cents and manifest cost-total mismatches. They also check existing target key uniqueness because standard Snowflake table primary keys would not enforce it.

After all gates pass, explicit typed projections convert DATE, TIMESTAMP_TZ and NUMBER(38,0). Three keyed merges and the success audit commit in one transaction. DDL is completed before that transaction. A merge or reconciliation error rolls back target changes and is raised again. Failure details are recorded in LOAD_RUNS. Empty initial target tables created by DDL can remain after a failed first attempt; that DDL is not part of the rollback guarantee.

Rows are compared by all projected typed values and the supplied source row hash. Unchanged rows retain their existing loaded_at and source lineage, which describe the last insert or content change. LOAD_RUNS records every applied batch separately. Rows missing from a later snapshot are retained. Concurrent writers, delete propagation and a general orchestration interface are outside this manual first-batch script.

## Expected output

On a fresh target, the first run returns status passed for all three entities:

| table_name | inserted | updated | unchanged | source_rows | reconciled_rows | target_total_rows |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| encounters | 53346 | 0 | 0 | 53346 | 53346 | 53346 |
| patients | 1171 | 0 | 0 | 1171 | 1171 | 1171 |
| payers | 10 | 0 | 0 | 10 | 10 | 10 |

Run the whole file again. Expect inserted=0 and updated=0, with unchanged=53346/1171/10 and the same source/reconciled/target counts. Each successful run has a distinct run_id. Bishal returned both result tables: initial run d03f650e-b196-4fa3-96f2-becc5759f96c passed with all rows inserted; rerun 9a852332-9d96-404f-be8b-a78851b1736d passed with all rows unchanged. Source and reconciled counts match in both. The displayed transcription does not include target_total_rows.

## Diagnosis

If execution fails, preserve the first error and inspect:

```sql
SELECT * FROM HEALTHCARE.RAW.LOAD_RUNS ORDER BY started_at DESC LIMIT 5;
-- Landing checks exist only in the same session that ran the load.
SELECT * FROM HEALTHCARE.RAW.BATCH_CHECKS
WHERE violations IS NULL OR violations <> 0 ORDER BY check_name;
```

COPY syntax, permissions or block compilation errors can occur before audit logging executes. Do not describe a locally reviewed script as live-tested until the user's Snowflake execution passes.

## Integrity boundary

The Python exporter independently verifies raw CSV byte SHA-256 and canonical per-row SHA-256. S3 publication uses conditional writes and SHA-256 checksums. This SQL script checks the manifest's declared identity against the previously verified value, records the declared file hashes, validates landing data and reconciles every typed row after merge. It does not independently re-hash raw S3 bytes or recreate Python's canonical JSON hash in Snowflake. A copied declared hash is lineage, not a fresh cryptographic verification. The isolated Snowflake correction/rollback demonstration is prepared in `04_snowflake_quality_proof.sql`; its live result, dbt models/tests and Airflow execution remain pending.

References:
- https://docs.snowflake.com/en/sql-reference/sql/copy-into-table
- https://docs.snowflake.com/en/sql-reference/sql/merge
- https://docs.snowflake.com/en/sql-reference/transactions
- https://docs.snowflake.com/en/developer-guide/snowflake-scripting/exceptions
