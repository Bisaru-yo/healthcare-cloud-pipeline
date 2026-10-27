# Isolated Snowflake warehouse quality proof

## Prerequisite and scope

The first-batch load and identical rerun have passed in Bishal's Snowflake account. First run `d03f650e-b196-4fa3-96f2-becc5759f96c` inserted 53346 encounters, 1171 patients and 10 payers. Rerun `9a852332-9d96-404f-be8b-a78851b1736d` inserted/updated zero rows and reported all records unchanged. The returned source and reconciled counts match for all three entities. These are user-provided result tables, not executions by the assistant.

Run all of `sql/04_snowflake_quality_proof.sql` in the same Snowflake workspace using Ctrl+Shift+Enter on Windows. The script creates/resets only dedicated `HEALTHCARE.QA` target copies from `HEALTHCARE.RAW`. It reads the existing S3 batch. All target merges and audit writes use fully qualified QA table names. RAW is read for fixture copies and exact before/after comparisons.

The script runs serially and assumes no other writes to the demo RAW or QA tables during the proof. It recreates three QA fixture tables and the QA procedure each time; do not use HEALTHCARE.QA for unrelated work. QA audit and proof-result histories are retained. The QA schema's small copied dataset consumes storage; it is a test fixture, not a new production dataset.

## Four cases

| Case | Expected behavior |
| --- | --- |
| late_correction | Increase one selected encounter cost by exactly 100 cents, update its canonical Python row hash and corresponding landing manifest sum; exactly one existing encounter changes, with zero inserts. |
| recovery | Reload the original S3 content through the same merges; exactly one encounter updates back, all projected content and lineage match the starting snapshot. loaded_at correctly reflects the write. |
| orphan_rejection | Assign that encounter a missing patient in landing; the actual loader gate reports one patient foreign-key violation, raises -20002, and all QA target values including timestamps stay unchanged. |
| transaction_rollback | Apply the valid cost correction, execute all three merge/reconciliation paths, then deliberately raise -20005 before commit. The same exception handler rolls back and re-raises; every QA target value including timestamps must match its pre-test snapshot. |

Expected failures are caught only by the harness and only for the exact expected codes. Unexpected errors stop the proof. Each case must satisfy explicit assertions before a passed result is saved. The final result contains four passed rows and `RAW_TABLES_UNCHANGED = TRUE`, after bidirectional row comparisons against RAW's starting content, lineage and timestamps.

This is warehouse fault injection, not a corrected batch published from PostgreSQL through S3. The modified CSV landing row and manifest cost total exist only inside the QA execution; the pinned source manifest identity and file-hash lineage still describe the original published batch. Do not present those declared hashes as hashes of the injected fixture. A new complete end-to-end correction batch remains a separate future proof if claimed.

## Reuse the actual loader

`python scripts/build_snowflake_proof.py` generates the QA SQL from the current `03_snowflake_load.sql`, reading the normalized official source to compute the corrected fixture's canonical SHA-256. It retains the loader's validation/merge/reconciliation body, qualifies persistent targets into QA, wraps it as a caller-rights procedure, and adds clearly marked injections. The builder asserts the three merge statements remain identical except for target qualification.

`python scripts/build_snowflake_proof.py --check` verifies the generated file has not drifted. The proof header records the SHA-256 of the loader it was built from. The S3 stage and JSON format are explicitly qualified in RAW; the CSV landing format is local to the QA session.

## Diagnosis and evidence

Return the final four-row result table. If the script stops, preserve the first error and inspect:

```sql
SELECT * FROM HEALTHCARE.QA.LOAD_RUNS ORDER BY started_at DESC LIMIT 8;
SELECT * FROM HEALTHCARE.QA.PROOF_RESULTS ORDER BY recorded_at DESC LIMIT 8;
-- Available only in the session that ran the proof:
SELECT * FROM HEALTHCARE.QA.BATCH_CHECKS
WHERE violations IS NULL OR violations <> 0;
```

The two expected rejected/rolled-back loader attempts have failed audit rows. That is correct test behavior; the harness records passed only after verifying the expected failure and preserved target values. Bishal executed the corrected QA script and returned four passed scenarios under proof_id 6ac73b61-7de9-4b38-ad9d-e93104d933e4, with RAW_TABLES_UNCHANGED=true. Do not mark its cases passed until the user's result is returned.

Official references:
- https://docs.snowflake.com/en/sql-reference/sql/merge
- https://docs.snowflake.com/en/sql-reference/transactions
- https://docs.snowflake.com/en/sql-reference/snowflake-scripting/raise
- https://docs.snowflake.com/en/sql-reference/sql/create-procedure

## Assertion syntax correction

The first user attempt stopped with an INTO compilation error at original line 387. All scalar assertions now select a named check_value column from a one-row derived table before INTO binds it to the scripting variable, and NULL results also fail the assertion. The corrected file has been regenerated and locally checked; the corrected script subsequently passed all four scenarios in Bishal's account. Rerun the whole corrected file to reset the QA fixtures and repeat all cases. RAW initial-load/replay evidence remains valid.
