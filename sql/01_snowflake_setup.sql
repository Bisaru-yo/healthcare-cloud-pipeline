-- Run in Snowflake Workspaces AFTER infra/setup-snowflake-reader.cmd succeeds.
-- The current account UJ02641 is in AWS_US_EAST_2; ACCOUNTADMIN was verified.
-- Initial setup only. Loading data begins after the AWS trust is finalized.
USE ROLE ACCOUNTADMIN;

CREATE WAREHOUSE IF NOT EXISTS HEALTHCARE_WH
    WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Healthcare pipeline portfolio warehouse';

CREATE DATABASE IF NOT EXISTS HEALTHCARE;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE.RAW;
CREATE SCHEMA IF NOT EXISTS HEALTHCARE.ANALYTICS;

-- IF NOT EXISTS preserves the generated identity/external ID on a rerun.
CREATE STORAGE INTEGRATION IF NOT EXISTS HEALTHCARE_S3_INT
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::009285528415:role/healthcare-snowflake-reader'
    STORAGE_ALLOWED_LOCATIONS = ('s3://bishal-healthcare-pipeline-bisar01/healthcare/');

-- Retain connection identifiers for the later Python connector/dbt setup.
SELECT
    CURRENT_ORGANIZATION_NAME() AS organization_name,
    CURRENT_ACCOUNT_NAME() AS account_name,
    CURRENT_ACCOUNT() AS account_locator,
    CURRENT_REGION() AS region,
    CURRENT_USER() AS snowflake_user,
    CURRENT_ROLE() AS active_role;

-- Return these two property values to finish the AWS role trust:
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID.
DESC INTEGRATION HEALTHCARE_S3_INT;
