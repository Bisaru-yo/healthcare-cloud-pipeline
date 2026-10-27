-- Run only AFTER the reader role trust has been updated to the actual
-- STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from Snowflake.
USE ROLE ACCOUNTADMIN;
USE DATABASE HEALTHCARE;
USE SCHEMA RAW;

CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_CSV
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ESCAPE_UNENCLOSED_FIELD = NONE
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('')
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
    ENCODING = 'UTF8';

CREATE FILE FORMAT IF NOT EXISTS HEALTHCARE_JSON
    TYPE = JSON;

CREATE STAGE IF NOT EXISTS HEALTHCARE_S3_STAGE
    URL = 's3://bishal-healthcare-pipeline-bisar01/healthcare/'
    STORAGE_INTEGRATION = HEALTHCARE_S3_INT
    FILE_FORMAT = HEALTHCARE_CSV;

-- Exactly the three data CSVs and completion manifest are expected.
LIST @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/;

-- Read only the completion manifest using its JSON format.
USE WAREHOUSE HEALTHCARE_WH;
SELECT
    $1:batch_id::VARCHAR AS batch_id,
    $1:status::VARCHAR AS status,
    $1:content_sha256::VARCHAR AS content_sha256,
    $1:tables.patients.row_count::NUMBER AS patient_rows,
    $1:tables.payers.row_count::NUMBER AS payer_rows,
    $1:tables.encounters.row_count::NUMBER AS encounter_rows
FROM @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/manifest.json
    (FILE_FORMAT => 'HEALTHCARE_JSON');
