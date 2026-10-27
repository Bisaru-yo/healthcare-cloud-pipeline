# dbt analytics layer

## What is prepared

Nine models: three staging views, current patient/payer dimensions, one encounter fact table, and three reporting views for monthly utilization, encounter-class mix and payer cost/coverage. They read HEALTHCARE.RAW and build in HEALTHCARE.ANALYTICS. The fact grain is one encounter UUID. Dimensions are current-state type 1; no history or deletion propagation is implied. Dimensions and the fact are rebuilt in full at this sample size, while the raw ingestion uses keyed merges.

There are 37 data tests: 28 generic tests and nine singular SQL tests. They cover source/model key uniqueness and not-null values, both dimension relationships, the six encounter classes observed in this sample, nonnegative cents, chronology, bidirectional dimension/fact content comparisons, raw-to-fact counts and cents, reporting grains and aggregate totals, and monthly class-share reconciliation. Allowed classes must be reviewed if an upstream data contract changes. Historical sample dates intentionally have no current-time freshness threshold.

Distinct patient counts apply within each reporting group and are not additive across groups. Costs and coverage are synthetic encounter amounts, not revenue, claim-line adjudication or a payment ledger. All authoritative reconciliation uses exact integer cents. Fractional ratios are display metrics. Dates use UTC: RAW stores normalized timestamp offsets and the generated service user has TIMEZONE=UTC.

## Install into the existing Windows project

Extract the updated project ZIP into a separate temporary folder. From its inner healthcare-cloud-pipeline folder, copy `dbt`, `scripts`, `infra`, `requirements-dbt.txt` and `.gitignore` into the existing project folder, allowing replacements. The existing .env, databases, data/batches and evidence stay in the existing folder. Use the same healthcare-cloud Anaconda environment.

```bat
python -m pip install -r requirements-dbt.txt
python scripts\setup_snowflake_dbt.py
```

Choose a private passphrase and repeat it when prompted. It is not your Google, AWS, PostgreSQL or Snowflake password. Input is hidden. The script creates an encrypted RSA 2048-bit PKCS#8 private key at `%USERPROFILE%\.healthcare-cloud\snowflake_dbt_key.p8`, outside the project/OneDrive directory. Keep the key and passphrase private and retain them for later dbt runs. Windows file access should remain limited to the user's account; the passphrase protects the key even when stored. No private key or passphrase is sent in the setup SQL.

The setup generates `local/setup_dbt_user.sql`, `local/dbt_key_location.json` and `dbt/profiles.yml` locally. These are ignored by git and excluded from the project ZIP. Rerunning reuses the existing key after decrypting it with the supplied passphrase, and preserves an existing dbt profile. It never silently replaces the key.

Open the generated SQL:

```bat
notepad local\setup_dbt_user.sql
```

Copy all of it into the existing Snowflake SQL editor and run all with Ctrl+Shift+Enter. It configures HEALTHCARE_DBT_USER (TYPE=SERVICE) with the locally generated public key and HEALTHCARE_TRANSFORMER role. That role can use the project warehouse, read the three RAW source tables, and create tables/views in ANALYTICS. It cannot change RAW, read S3 through the stage, or administer the account. ACCOUNTADMIN is used only to configure this initial user/role. The role is granted to SYSADMIN for the standard administrative hierarchy. No public/private key needs to be pasted into chat.

The first run uses CREATE USER IF NOT EXISTS and configures its project key. Subsequent setup applies the same key again. Actual key rotation, moving keys between machines or reusing an unrelated pre-existing user requires a deliberate separate procedure.

## Connect and build

Return to the same terminal:

```bat
python scripts\run_dbt.py debug
```

Enter the key passphrase chosen earlier. Expect a successful connection check. If it fails, preserve the first error. The default account parameter `pbsfajr-lw57115` uses the organization-account identifier returned by Snowflake. A first attempt with the locator/region hostname failed SSL hostname verification; using the preferred identifier passed without disabling SSL verification. For an existing generated profile, replace only `uj02641.us-east-2` with `pbsfajr-lw57115` in `dbt/profiles.yml` to retain this default across terminal sessions. To use another account identifier, set SNOWFLAKE_ACCOUNT in the terminal; the template reads that variable.

After debug succeeds:

```bat
python scripts\run_dbt.py build
```

Enter the same key passphrase. The wrapper uses the official dbt Python runner, passing a private environment variable that dbt masks from logs. It removes a passphrase it prompted for when the invocation ends. It records outcome/time, build status counts and fresh run_results.json in a new evidence/dbt_* directory. A failed invocation returns a nonzero exit code and is not labeled as passed. An older run_results.json is not attached if a new run never wrote one.

Expect nine models and 37 data tests to succeed. Then optionally generate model documentation:

```bat
python scripts\run_dbt.py docs-generate
```

The docs artifacts are in dbt/target. Use `sql/05_reporting_queries.sql` in Snowflake to inspect all three reporting views. The fact should contain 53346 rows, total_claim_cost_cents=686831295 and payer_coverage_cents=330739157 for this snapshot. Cost amounts divide cents by 100; no measured revenue or business outcome is claimed.

## Validation completed locally

Python 3.12 installed dbt-core 1.12.6 and dbt-snowflake 1.12.4, with a clean pip dependency check. The official dbt parser passed on the project with nine models, three sources and 37 tests; no cloud connection was made. The encrypted-key helper's regression checks verify encryption, absence of private material/passphrases in generated project files, reuse of an existing key/profile, and protection of an existing key when the wrong passphrase is supplied. The complete Python suite passed 30 tests in that environment.

An additional offline proof parses SQL in the Snowflake dialect, transpiles it with SQLGlot and runs it in DuckDB on the entire normalized sample. All nine models and 37 data-check equivalents passed; the fact and both cent totals reconcile, and the monthly utilization view contains 15827 groups. Deliberately dropped/duplicated fact rows and an orphan payer triggered the expected tests. After rollback all checks passed again. This is not dbt execution in Snowflake or a substitute for the user's `dbt build`.

To repeat that optional offline proof:

```bat
python -m pip install -r requirements-dbt-validation.txt
python scripts\verify_dbt_locally.py
```

The two direct dbt packages and the key helper's cryptography dependency are pinned. Transitive dependency versions are resolved by pip; reviewed local versions and outcomes are recorded under evidence/dbt_local. Bishal subsequently verified the real Snowflake connection and build: nine models succeeded, all 37 data tests passed, with zero warnings/errors/skips. The original returned terminal log is retained in evidence/cloud_handoff/dbt_build_windows.txt. Report totals and the payer sample subsequently reconciled, the real seven-stage Airflow workflow passed, and dbt docs generation wrote its catalog successfully. Reviewed evidence is under `evidence/cloud_handoff`; generated docs themselves remain local under ignored `dbt/target`.

References:
- https://docs.getdbt.com/reference/programmatic-invocations
- https://docs.getdbt.com/docs/build/data-tests
- https://docs.snowflake.com/en/user-guide/key-pair-auth
- https://docs.snowflake.com/en/sql-reference/sql/create-user
