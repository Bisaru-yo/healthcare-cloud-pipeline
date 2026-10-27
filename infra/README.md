# Snowflake read-only integration setup

This directory configures the next stage: AWS S3 to Snowflake. The manual upload was already verified using the existing `healthcare-cloud` temporary browser-login profile. No separate uploader role was created or verified.

Confirmed resources:

| Setting | Value |
| --- | --- |
| AWS account | `009285528415` |
| AWS region | Ohio, `us-east-2` |
| S3 bucket | `bishal-healthcare-pipeline-bisar01` |
| Project prefix | `healthcare/` |
| Verified batch | `20261026T235514_a3bfed17dc09` |
| Snowflake locator | `UJ02641` |
| Snowflake region | `AWS_US_EAST_2` |
| Verified Snowflake setup role | `ACCOUNTADMIN` |

## Current setup files

1. `snowflake-reader-trust-initial.json`: temporary trust to this AWS account, with a placeholder external ID. This follows Snowflake's bootstrap flow. It does not yet grant Snowflake access.
2. `snowflake-reader-policy.json`: the four S3 read/list operations required for the project's prefix. No S3 write or delete permissions are granted.
3. `setup-snowflake-reader.cmd`: verifies the setup account, creates one role named `healthcare-snowflake-reader`, and attaches that read-only policy. It does not assume the role or upload/delete data.
4. `../sql/01_snowflake_setup.sql`: creates an initially suspended X-Small warehouse, project database/schemas and a storage integration, then describes the integration.
5. `../sql/02_snowflake_stage.sql`: creates the CSV/JSON file formats and S3 stage, then lists the completed batch and queries its manifest. Run this only after the final trust update.

Run the command file from the project folder in Anaconda Prompt:

```bat
infra\setup-snowflake-reader.cmd
```

If it fails, stop and inspect the exact error. If the role already exists, the script stops rather than overwriting a possibly unrelated role. If a failure happens after creation, resume the relevant command only after checking the role. The setup profile must have the IAM creation permissions and a valid login. Renew an expired session with `aws login --profile healthcare-cloud`.

After successful AWS setup, run all statements in `sql/01_snowflake_setup.sql` in Snowflake Workspaces. Record `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` from `DESC INTEGRATION HEALTHCARE_S3_INT`. The IAM role trust must then be changed to allow that exact Snowflake IAM user to call `sts:AssumeRole`, conditioned on that exact external ID. These account-generated values cannot be guessed. The final trust update is the next step, not something the initial command script does.

## Final trust from the returned integration properties

The user returned `DESC INTEGRATION HEALTHCARE_S3_INT` successfully. `snowflake-reader-trust-final.json` contains that exact Snowflake IAM user ARN and external ID. It is specific to this integration; a recreated integration may generate a different external ID.

From the project folder, apply the final trust to the existing reader role:

```bat
aws iam update-assume-role-policy --role-name healthcare-snowflake-reader --policy-document file://infra/snowflake-reader-trust-final.json --profile healthcare-cloud --no-cli-pager
```

Success normally returns no text. Then run all of `sql/02_snowflake_stage.sql` in the existing Snowflake workspace. Expect four listed objects and manifest counts of 1171 patients, 10 payers and 53346 encounters, with status `complete`.

The JSON documents and scope are checked locally. The storage integration exists based on user-provided Snowflake output. Bishal subsequently returned a successful staged manifest query with the expected batch identity and all three counts, verifying the integration-to-S3 read path. The data load is prepared in `../sql/03_snowflake_load.sql` and has not yet been executed. No long-term AWS credentials are included.

Reference: https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration
