# PostgreSQL extraction and S3 batch publication

## Current state

Bishal verified the PostgreSQL source load and identical rerun on Windows. The rerun reconciled all 1,171 patients, 10 payers and 53,346 encounters with zero inserts or updates. PostgreSQL is reachable on `127.0.0.1:15432`; Docker maps that to container port `5432`.

The exporter and S3 uploader are implemented. All 27 local tests pass. A full-sample SQLite export was verified twice with identical payload hashes, and its manifest and summary are in `evidence/extraction_local`. Bishal subsequently verified the PostgreSQL export and AWS S3 publication from Windows. First upload: four objects uploaded; identical retry: zero uploaded, four reused. The account probe for Snowflake locator UJ02641 returned AWS_US_EAST_2 and ACCOUNTADMIN. Role/integration/warehouse setup is prepared but execution is pending. The S3 unit tests use an explicitly in-memory fake, not AWS or a service emulator.

## Export from PostgreSQL

In the same Anaconda Prompt where `DATABASE_URL` is configured, and with Docker/PostgreSQL running:

```bat
python -m unittest discover -s tests -v
python -m pipeline.export --engine postgres
```

No new packages are required beyond the already installed PostgreSQL connector. The command prints the batch directory and row counts. It reads the database; it does not reload source CSVs or modify source rows.

Each new batch folder under `data/batches` contains:

| File | Contents |
| --- | --- |
| `patients.csv` | Normalized source patient projection plus stored row hash |
| `payers.csv` | Normalized payer projection plus stored row hash |
| `encounters.csv` | Normalized encounters with integer cent amounts and stored row hash |
| `manifest.json` | Batch ID, source engine, schema version, timestamps, counts, monetary totals, file sizes and SHA-256 hashes |

The exporter uses one PostgreSQL **repeatable-read, read-only transaction** across all tables, avoiding separate snapshots for parent and child tables. It verifies source row hashes while writing, checks exported counts and amount totals against queries inside the same transaction, then independently verifies the saved CSVs. It checks file hashes, keys, both relationships, counts, monetary totals and row content hashes. Only then is the temporary folder renamed to the final batch folder. An interrupted `.partial_...` folder is not a complete batch.

The export is a full snapshot of the normalized source projection. UUID sorting and explicit UTF-8/LF CSV output make unchanged payloads deterministic across runs. Each new export receives a new batch ID and timestamp. Reuse the **same local batch directory** when retrying an upload; do not re-export solely to retry S3 publication. Creating a new batch from unchanged data is permitted, with warehouse keyed merges handling repeated business records later.

Default source host port is now `15432`, matching the successful Windows setup. `POSTGRES_HOST_PORT` in `.env` can override the Compose host port; update `DATABASE_URL` separately if the port changes.

## AWS prerequisites

Use an AWS account and an authorized local-development identity. Install the current AWS CLI v2 from its official Windows installer. Configure a private general-purpose S3 bucket in the selected region; block public access and keep ACLs disabled. Use the intended region consistently with Snowflake later. Provisioning and actual AWS authentication have not been performed by this handoff.

Prefer temporary credentials. Current AWS CLI supports browser-based `aws login` with an authorized console identity, or `aws configure sso` for IAM Identity Center. AWS CLI version 2.32.0 or later is needed for `aws login`. IAM identities using it require the documented `SignInLocalDevelopmentAccess` permission. Boto3's login provider requires CRT, included by `requirements-aws.txt`. Choose the login method to match the actual account setup; the uploader uses the SDK's normal credential chain and never reads a password or access key from the project files.

Install the uploader dependency only when connecting AWS:

```bat
python -m pip install -r requirements-aws.txt
```

IAM requirements for this uploader: `s3:PutObject` and `s3:GetObject` on objects under the dedicated bucket/prefix. `HeadObject` uses the `GetObject` permission. Listing objects separately requires `s3:ListBucket` on the bucket, restricted to the project prefix where appropriate. Creating/configuring a bucket is a separate operation and is not performed by this code. Add no public-read ACL. Use the account's billing limits and actual pricing; no zero-cost guarantee is made.

## Publish one verified batch

Replace the directory and bucket placeholders after actual account setup. The example region is `us-east-2`; use the real bucket region.

```bat
python -m pipeline.publish --batch "data\batches\YOUR_BATCH_ID" --bucket YOUR_BUCKET_NAME --region us-east-2
```

The uploader verifies the entire local batch before making an S3 call. Its output keys are:

```text
healthcare/BATCH_ID/patients.csv
healthcare/BATCH_ID/payers.csv
healthcare/BATCH_ID/encounters.csv
healthcare/BATCH_ID/manifest.json
```

The order matters: the three data files are published and checked first; `manifest.json` is written last as the completion signal. Downstream loads must discover completed manifests, not assume that an S3 prefix containing CSV files is ready.

Each PUT includes a SHA-256 checksum, SHA-256 metadata and SSE-S3 encryption. Conditional creation (`IfNoneMatch='*'`) prevents overwrites. After a successful PUT or a precondition failure for an existing object, the uploader checks the remote length, SHA-256 metadata and S3-returned SHA-256 checksum. A conflict stops publication. The same batch can be retried: matching objects are reused, and missing ones are uploaded. SDK network retries are bounded; retrying the command after an interrupted batch is supported. Incomplete prefixes are left intact for recovery.

On success, `s3_receipt.json` is saved alongside the local batch, with object keys/checksums, upload-versus-reuse actions and the manifest URI. Retrying the same completed batch should report `uploaded: 0` and `reused: 4`. The receipt is execution evidence only after a real AWS run. No AWS services are contacted by the local tests.

## Verification sequence and current next action

1. Verified: PostgreSQL export on Bishal's machine has the expected counts and content checksum.
2. Verified: temporary CLI browser login and private bucket bishal-healthcare-pipeline-bisar01 in Ohio/us-east-2.
3. Verified by user-provided output: all four objects were published successfully. The original receipt is on Bishal's machine.
4. Verified: the same batch retry uploaded zero objects and reused all four.
5. Current next step: `infra/setup-snowflake-reader.cmd`, followed by `sql/01_snowflake_setup.sql`, the generated trust update, and the Snowflake S3 stage and batch landing load.

## Primary references

- PostgreSQL snapshot isolation: https://www.postgresql.org/docs/current/transaction-iso.html
- S3 conditional PUT/checksum options: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/put_object.html
- S3 object verification: https://docs.aws.amazon.com/boto3/latest/reference/services/s3/client/head_object.html
- AWS CLI Windows installation: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
- Browser login: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-sign-in.html
- Boto3 login provider CRT support: https://docs.aws.amazon.com/sdkref/latest/guide/feature-login-credentials.html
