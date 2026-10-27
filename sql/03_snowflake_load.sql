-- First verified S3 batch only. Run the WHOLE file twice in one workspace.
-- Full snapshot -> typed landing -> validated keyed merge. No target deletes.
-- Run serially: one loader at a time. No DDL occurs inside the merge transaction.
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE HEALTHCARE;
USE SCHEMA RAW;
ALTER SESSION SET TIMEZONE = 'UTC';
ALTER SESSION SET ERROR_ON_NONDETERMINISTIC_MERGE = TRUE;

-- Empty CSV fields remain empty strings in landing; optional dates become NULL
-- only in the explicit typed projection. Keep the existing shared format intact.
CREATE OR REPLACE TEMPORARY FILE FORMAT BATCH_CSV
    TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' ESCAPE_UNENCLOSED_FIELD = NONE
    EMPTY_FIELD_AS_NULL = FALSE NULL_IF = ()
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE ENCODING = 'UTF8';

CREATE TABLE IF NOT EXISTS LOAD_RUNS (
    run_id VARCHAR NOT NULL, batch_id VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL, status VARCHAR NOT NULL,
    started_at TIMESTAMP_TZ NOT NULL, finished_at TIMESTAMP_TZ NOT NULL,
    metrics VARIANT, error_code NUMBER, error_message VARCHAR
);

CREATE TABLE IF NOT EXISTS PATIENTS (
    id VARCHAR NOT NULL,
    birthdate DATE,
    deathdate DATE,
    gender VARCHAR,
    city VARCHAR,
    state VARCHAR,
    row_hash VARCHAR NOT NULL,
    source_batch_id VARCHAR NOT NULL,
    source_file_sha256 VARCHAR NOT NULL,
    source_content_sha256 VARCHAR NOT NULL,
    loaded_at TIMESTAMP_TZ NOT NULL
);

CREATE TABLE IF NOT EXISTS PAYERS (
    id VARCHAR NOT NULL,
    name VARCHAR,
    ownership VARCHAR,
    row_hash VARCHAR NOT NULL,
    source_batch_id VARCHAR NOT NULL,
    source_file_sha256 VARCHAR NOT NULL,
    source_content_sha256 VARCHAR NOT NULL,
    loaded_at TIMESTAMP_TZ NOT NULL
);

CREATE TABLE IF NOT EXISTS ENCOUNTERS (
    id VARCHAR NOT NULL,
    start_at TIMESTAMP_TZ,
    stop_at TIMESTAMP_TZ,
    patient_id VARCHAR,
    payer_id VARCHAR,
    encounter_class VARCHAR,
    encounter_code VARCHAR,
    total_claim_cost_cents NUMBER(38,0),
    payer_coverage_cents NUMBER(38,0),
    row_hash VARCHAR NOT NULL,
    source_batch_id VARCHAR NOT NULL,
    source_file_sha256 VARCHAR NOT NULL,
    source_content_sha256 VARCHAR NOT NULL,
    loaded_at TIMESTAMP_TZ NOT NULL
);


-- DDL and file loading are outside the target transaction.
EXECUTE IMMEDIATE $$
DECLARE
    run_key VARCHAR DEFAULT UUID_STRING();
    run_started TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP();
    transaction_open BOOLEAN DEFAULT FALSE;
    failures NUMBER DEFAULT 0;
    manifest_error EXCEPTION (-20001, 'Manifest identity mismatch; target data was not changed.');
    landing_error EXCEPTION (-20002, 'Batch quality failed. Query BATCH_CHECKS for nonzero violations.');
    target_error EXCEPTION (-20003, 'Target reconciliation failed; all three merges were rolled back.');
    result_rows RESULTSET;
BEGIN
    CREATE OR REPLACE TEMPORARY TABLE BATCH_MANIFEST (payload VARIANT);
    CREATE OR REPLACE TEMPORARY TABLE BATCH_CHECKS (check_name VARCHAR, violations NUMBER);
    CREATE OR REPLACE TEMPORARY TABLE BATCH_METRICS (
        table_name VARCHAR, inserted NUMBER, updated NUMBER, unchanged NUMBER,
        source_rows NUMBER, reconciled_rows NUMBER, target_total_rows NUMBER
    );

    CREATE OR REPLACE TEMPORARY TABLE BATCH_PATIENTS (
        id VARCHAR,
        birthdate VARCHAR,
        deathdate VARCHAR,
        gender VARCHAR,
        city VARCHAR,
        state VARCHAR,
        row_hash VARCHAR
    );

    CREATE OR REPLACE TEMPORARY TABLE BATCH_PAYERS (
        id VARCHAR,
        name VARCHAR,
        ownership VARCHAR,
        row_hash VARCHAR
    );

    CREATE OR REPLACE TEMPORARY TABLE BATCH_ENCOUNTERS (
        id VARCHAR,
        start_text VARCHAR,
        stop_text VARCHAR,
        patient VARCHAR,
        payer VARCHAR,
        encounterclass VARCHAR,
        code VARCHAR,
        total_claim_cost_cents VARCHAR,
        payer_coverage_cents VARCHAR,
        row_hash VARCHAR
    );

    COPY INTO BATCH_MANIFEST
        FROM @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/
        FILES = ('manifest.json') FILE_FORMAT = (FORMAT_NAME = 'HEALTHCARE_JSON')
        ON_ERROR = ABORT_STATEMENT FORCE = TRUE;

    SELECT COUNT(*) INTO :failures FROM BATCH_MANIFEST
    WHERE payload:schema_version::NUMBER = 1
      AND payload:batch_id::VARCHAR = '20261026T235514_a3bfed17dc09'
      AND payload:status::VARCHAR = 'complete'
      AND payload:extraction_mode::VARCHAR = 'full_snapshot'
      AND payload:content_sha256::VARCHAR = 'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c'
      AND (SELECT COUNT(*) FROM BATCH_MANIFEST) = 1;
    IF (failures <> 1) THEN
        RAISE manifest_error;
    END IF;

    COPY INTO BATCH_PATIENTS
        FROM @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/
        FILES = ('patients.csv') FILE_FORMAT = (FORMAT_NAME = 'BATCH_CSV')
        ON_ERROR = ABORT_STATEMENT FORCE = TRUE;

    COPY INTO BATCH_PAYERS
        FROM @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/
        FILES = ('payers.csv') FILE_FORMAT = (FORMAT_NAME = 'BATCH_CSV')
        ON_ERROR = ABORT_STATEMENT FORCE = TRUE;

    COPY INTO BATCH_ENCOUNTERS
        FROM @HEALTHCARE_S3_STAGE/20261026T235514_a3bfed17dc09/
        FILES = ('encounters.csv') FILE_FORMAT = (FORMAT_NAME = 'BATCH_CSV')
        ON_ERROR = ABORT_STATEMENT FORCE = TRUE;

    INSERT INTO BATCH_CHECKS
    SELECT 'patients_count', (SELECT ABS(COUNT(*) - (SELECT payload:tables.patients.row_count::NUMBER FROM BATCH_MANIFEST)) FROM BATCH_PATIENTS)
    UNION ALL
    SELECT 'patients_empty', (SELECT IFF(COUNT(*) = 0, 1, 0) FROM BATCH_PATIENTS)
    UNION ALL
    SELECT 'patients_duplicate_ids', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM BATCH_PATIENTS)
    UNION ALL
    SELECT 'patients_key_and_hash', (SELECT COUNT(*) FROM BATCH_PATIENTS WHERE id IS NULL OR NOT REGEXP_LIKE(id, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}') OR row_hash IS NULL OR NOT REGEXP_LIKE(row_hash, '[0-9a-f]{64}'))
    UNION ALL
    SELECT 'patients_null_fields', (SELECT COUNT(*) FROM BATCH_PATIENTS WHERE id IS NULL OR birthdate IS NULL OR deathdate IS NULL OR gender IS NULL OR city IS NULL OR state IS NULL OR row_hash IS NULL)
    UNION ALL
    SELECT 'patients_existing_target_duplicates', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM PATIENTS)
    UNION ALL
    SELECT 'payers_count', (SELECT ABS(COUNT(*) - (SELECT payload:tables.payers.row_count::NUMBER FROM BATCH_MANIFEST)) FROM BATCH_PAYERS)
    UNION ALL
    SELECT 'payers_empty', (SELECT IFF(COUNT(*) = 0, 1, 0) FROM BATCH_PAYERS)
    UNION ALL
    SELECT 'payers_duplicate_ids', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM BATCH_PAYERS)
    UNION ALL
    SELECT 'payers_key_and_hash', (SELECT COUNT(*) FROM BATCH_PAYERS WHERE id IS NULL OR NOT REGEXP_LIKE(id, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}') OR row_hash IS NULL OR NOT REGEXP_LIKE(row_hash, '[0-9a-f]{64}'))
    UNION ALL
    SELECT 'payers_null_fields', (SELECT COUNT(*) FROM BATCH_PAYERS WHERE id IS NULL OR name IS NULL OR ownership IS NULL OR row_hash IS NULL)
    UNION ALL
    SELECT 'payers_existing_target_duplicates', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM PAYERS)
    UNION ALL
    SELECT 'encounters_count', (SELECT ABS(COUNT(*) - (SELECT payload:tables.encounters.row_count::NUMBER FROM BATCH_MANIFEST)) FROM BATCH_ENCOUNTERS)
    UNION ALL
    SELECT 'encounters_empty', (SELECT IFF(COUNT(*) = 0, 1, 0) FROM BATCH_ENCOUNTERS)
    UNION ALL
    SELECT 'encounters_duplicate_ids', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM BATCH_ENCOUNTERS)
    UNION ALL
    SELECT 'encounters_key_and_hash', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE id IS NULL OR NOT REGEXP_LIKE(id, '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}') OR row_hash IS NULL OR NOT REGEXP_LIKE(row_hash, '[0-9a-f]{64}'))
    UNION ALL
    SELECT 'encounters_null_fields', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE id IS NULL OR start_text IS NULL OR stop_text IS NULL OR patient IS NULL OR payer IS NULL OR encounterclass IS NULL OR code IS NULL OR total_claim_cost_cents IS NULL OR payer_coverage_cents IS NULL OR row_hash IS NULL)
    UNION ALL
    SELECT 'encounters_existing_target_duplicates', (SELECT COUNT(*) - COUNT(DISTINCT id) FROM ENCOUNTERS)
    UNION ALL
    SELECT 'patient_dates', (SELECT COUNT(*) FROM BATCH_PATIENTS WHERE TRY_TO_DATE(birthdate, 'YYYY-MM-DD') IS NULL OR (deathdate <> '' AND (TRY_TO_DATE(deathdate, 'YYYY-MM-DD') IS NULL OR TRY_TO_DATE(deathdate, 'YYYY-MM-DD') < TRY_TO_DATE(birthdate, 'YYYY-MM-DD'))))
    UNION ALL
    SELECT 'patient_required', (SELECT COUNT(*) FROM BATCH_PATIENTS WHERE TRIM(gender) = '')
    UNION ALL
    SELECT 'payer_required', (SELECT COUNT(*) FROM BATCH_PAYERS WHERE TRIM(name) = '')
    UNION ALL
    SELECT 'encounter_required', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE TRIM(encounterclass) = '' OR TRIM(code) = '')
    UNION ALL
    SELECT 'encounter_dates', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE TRY_TO_TIMESTAMP_TZ(start_text) IS NULL OR NOT REGEXP_LIKE(start_text, '.*[+]00:00') OR (stop_text <> '' AND (TRY_TO_TIMESTAMP_TZ(stop_text) IS NULL OR NOT REGEXP_LIKE(stop_text, '.*[+]00:00') OR TRY_TO_TIMESTAMP_TZ(stop_text) < TRY_TO_TIMESTAMP_TZ(start_text))))
    UNION ALL
    SELECT 'patient_foreign_keys', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS e LEFT JOIN BATCH_PATIENTS p ON e.patient = p.id WHERE p.id IS NULL)
    UNION ALL
    SELECT 'payer_foreign_keys', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS e LEFT JOIN BATCH_PAYERS p ON e.payer = p.id WHERE p.id IS NULL)
    UNION ALL
    SELECT 'total_claim_cost_cents_valid', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE NOT REGEXP_LIKE(total_claim_cost_cents, '[0-9]+') OR TRY_TO_NUMBER(total_claim_cost_cents, 38, 0) IS NULL OR TRY_TO_NUMBER(total_claim_cost_cents, 38, 0) > 9223372036854775807)
    UNION ALL
    SELECT 'total_claim_cost_cents_sum', (SELECT ABS(COALESCE(SUM(TRY_TO_NUMBER(total_claim_cost_cents, 38, 0)), 0) - (SELECT payload:tables.encounters.amount_totals_cents.total_claim_cost_cents::NUMBER FROM BATCH_MANIFEST)) FROM BATCH_ENCOUNTERS)
    UNION ALL
    SELECT 'payer_coverage_cents_valid', (SELECT COUNT(*) FROM BATCH_ENCOUNTERS WHERE NOT REGEXP_LIKE(payer_coverage_cents, '[0-9]+') OR TRY_TO_NUMBER(payer_coverage_cents, 38, 0) IS NULL OR TRY_TO_NUMBER(payer_coverage_cents, 38, 0) > 9223372036854775807)
    UNION ALL
    SELECT 'payer_coverage_cents_sum', (SELECT ABS(COALESCE(SUM(TRY_TO_NUMBER(payer_coverage_cents, 38, 0)), 0) - (SELECT payload:tables.encounters.amount_totals_cents.payer_coverage_cents::NUMBER FROM BATCH_MANIFEST)) FROM BATCH_ENCOUNTERS);

    SELECT COUNT(*) INTO :failures FROM BATCH_CHECKS
        WHERE violations IS NULL OR violations <> 0;
    IF (failures <> 0) THEN
        RAISE landing_error;
    END IF;

    CREATE OR REPLACE TEMPORARY VIEW TYPED_PATIENTS AS
    SELECT
        id AS id,
        TO_DATE(birthdate, 'YYYY-MM-DD') AS birthdate,
        TO_DATE(NULLIF(deathdate, ''), 'YYYY-MM-DD') AS deathdate,
        gender AS gender,
        city AS city,
        state AS state,
        row_hash AS row_hash,
        '20261026T235514_a3bfed17dc09' AS source_batch_id,
        (SELECT payload:tables.patients.sha256::VARCHAR FROM BATCH_MANIFEST) AS source_file_sha256,
        'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c' AS source_content_sha256
    FROM BATCH_PATIENTS;

    CREATE OR REPLACE TEMPORARY VIEW TYPED_PAYERS AS
    SELECT
        id AS id,
        name AS name,
        ownership AS ownership,
        row_hash AS row_hash,
        '20261026T235514_a3bfed17dc09' AS source_batch_id,
        (SELECT payload:tables.payers.sha256::VARCHAR FROM BATCH_MANIFEST) AS source_file_sha256,
        'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c' AS source_content_sha256
    FROM BATCH_PAYERS;

    CREATE OR REPLACE TEMPORARY VIEW TYPED_ENCOUNTERS AS
    SELECT
        id AS id,
        TO_TIMESTAMP_TZ(start_text) AS start_at,
        TO_TIMESTAMP_TZ(NULLIF(stop_text, '')) AS stop_at,
        patient AS patient_id,
        payer AS payer_id,
        encounterclass AS encounter_class,
        code AS encounter_code,
        TO_NUMBER(total_claim_cost_cents, 38, 0) AS total_claim_cost_cents,
        TO_NUMBER(payer_coverage_cents, 38, 0) AS payer_coverage_cents,
        row_hash AS row_hash,
        '20261026T235514_a3bfed17dc09' AS source_batch_id,
        (SELECT payload:tables.encounters.sha256::VARCHAR FROM BATCH_MANIFEST) AS source_file_sha256,
        'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c' AS source_content_sha256
    FROM BATCH_ENCOUNTERS;


    -- All persistent target changes and the success audit use one transaction.
    BEGIN TRANSACTION;
    transaction_open := TRUE;

    INSERT INTO BATCH_METRICS
    SELECT 'patients',
        COALESCE(COUNT_IF(t.id IS NULL), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.birthdate, s.birthdate) AND EQUAL_NULL(t.deathdate, s.deathdate) AND EQUAL_NULL(t.gender, s.gender) AND EQUAL_NULL(t.city, s.city) AND EQUAL_NULL(t.state, s.state) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.birthdate, s.birthdate) AND EQUAL_NULL(t.deathdate, s.deathdate) AND EQUAL_NULL(t.gender, s.gender) AND EQUAL_NULL(t.city, s.city) AND EQUAL_NULL(t.state, s.state) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COUNT(*), 0, 0
    FROM TYPED_PATIENTS s LEFT JOIN PATIENTS t ON t.id = s.id;

    MERGE INTO PATIENTS t USING TYPED_PATIENTS s ON t.id = s.id
    WHEN MATCHED AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.birthdate, s.birthdate) AND EQUAL_NULL(t.deathdate, s.deathdate) AND EQUAL_NULL(t.gender, s.gender) AND EQUAL_NULL(t.city, s.city) AND EQUAL_NULL(t.state, s.state) AND EQUAL_NULL(t.row_hash, s.row_hash)) THEN UPDATE SET
        birthdate = s.birthdate,
        deathdate = s.deathdate,
        gender = s.gender,
        city = s.city,
        state = s.state,
        row_hash = s.row_hash,
        source_batch_id = s.source_batch_id,
        source_file_sha256 = s.source_file_sha256,
        source_content_sha256 = s.source_content_sha256,
        loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (id, birthdate, deathdate, gender, city, state, row_hash, source_batch_id, source_file_sha256, source_content_sha256, loaded_at)
    VALUES
        (s.id, s.birthdate, s.deathdate, s.gender, s.city, s.state, s.row_hash, s.source_batch_id, s.source_file_sha256, s.source_content_sha256, CURRENT_TIMESTAMP());

    UPDATE BATCH_METRICS SET
        reconciled_rows = (SELECT COUNT(*) FROM TYPED_PATIENTS s
            JOIN PATIENTS t ON t.id = s.id WHERE EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.birthdate, s.birthdate) AND EQUAL_NULL(t.deathdate, s.deathdate) AND EQUAL_NULL(t.gender, s.gender) AND EQUAL_NULL(t.city, s.city) AND EQUAL_NULL(t.state, s.state) AND EQUAL_NULL(t.row_hash, s.row_hash)),
        target_total_rows = (SELECT COUNT(*) FROM PATIENTS)
    WHERE table_name = 'patients';

    INSERT INTO BATCH_METRICS
    SELECT 'payers',
        COALESCE(COUNT_IF(t.id IS NULL), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.name, s.name) AND EQUAL_NULL(t.ownership, s.ownership) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.name, s.name) AND EQUAL_NULL(t.ownership, s.ownership) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COUNT(*), 0, 0
    FROM TYPED_PAYERS s LEFT JOIN PAYERS t ON t.id = s.id;

    MERGE INTO PAYERS t USING TYPED_PAYERS s ON t.id = s.id
    WHEN MATCHED AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.name, s.name) AND EQUAL_NULL(t.ownership, s.ownership) AND EQUAL_NULL(t.row_hash, s.row_hash)) THEN UPDATE SET
        name = s.name,
        ownership = s.ownership,
        row_hash = s.row_hash,
        source_batch_id = s.source_batch_id,
        source_file_sha256 = s.source_file_sha256,
        source_content_sha256 = s.source_content_sha256,
        loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (id, name, ownership, row_hash, source_batch_id, source_file_sha256, source_content_sha256, loaded_at)
    VALUES
        (s.id, s.name, s.ownership, s.row_hash, s.source_batch_id, s.source_file_sha256, s.source_content_sha256, CURRENT_TIMESTAMP());

    UPDATE BATCH_METRICS SET
        reconciled_rows = (SELECT COUNT(*) FROM TYPED_PAYERS s
            JOIN PAYERS t ON t.id = s.id WHERE EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.name, s.name) AND EQUAL_NULL(t.ownership, s.ownership) AND EQUAL_NULL(t.row_hash, s.row_hash)),
        target_total_rows = (SELECT COUNT(*) FROM PAYERS)
    WHERE table_name = 'payers';

    INSERT INTO BATCH_METRICS
    SELECT 'encounters',
        COALESCE(COUNT_IF(t.id IS NULL), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.start_at, s.start_at) AND EQUAL_NULL(t.stop_at, s.stop_at) AND EQUAL_NULL(t.patient_id, s.patient_id) AND EQUAL_NULL(t.payer_id, s.payer_id) AND EQUAL_NULL(t.encounter_class, s.encounter_class) AND EQUAL_NULL(t.encounter_code, s.encounter_code) AND EQUAL_NULL(t.total_claim_cost_cents, s.total_claim_cost_cents) AND EQUAL_NULL(t.payer_coverage_cents, s.payer_coverage_cents) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COALESCE(COUNT_IF(t.id IS NOT NULL AND (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.start_at, s.start_at) AND EQUAL_NULL(t.stop_at, s.stop_at) AND EQUAL_NULL(t.patient_id, s.patient_id) AND EQUAL_NULL(t.payer_id, s.payer_id) AND EQUAL_NULL(t.encounter_class, s.encounter_class) AND EQUAL_NULL(t.encounter_code, s.encounter_code) AND EQUAL_NULL(t.total_claim_cost_cents, s.total_claim_cost_cents) AND EQUAL_NULL(t.payer_coverage_cents, s.payer_coverage_cents) AND EQUAL_NULL(t.row_hash, s.row_hash))), 0),
        COUNT(*), 0, 0
    FROM TYPED_ENCOUNTERS s LEFT JOIN ENCOUNTERS t ON t.id = s.id;

    MERGE INTO ENCOUNTERS t USING TYPED_ENCOUNTERS s ON t.id = s.id
    WHEN MATCHED AND NOT (EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.start_at, s.start_at) AND EQUAL_NULL(t.stop_at, s.stop_at) AND EQUAL_NULL(t.patient_id, s.patient_id) AND EQUAL_NULL(t.payer_id, s.payer_id) AND EQUAL_NULL(t.encounter_class, s.encounter_class) AND EQUAL_NULL(t.encounter_code, s.encounter_code) AND EQUAL_NULL(t.total_claim_cost_cents, s.total_claim_cost_cents) AND EQUAL_NULL(t.payer_coverage_cents, s.payer_coverage_cents) AND EQUAL_NULL(t.row_hash, s.row_hash)) THEN UPDATE SET
        start_at = s.start_at,
        stop_at = s.stop_at,
        patient_id = s.patient_id,
        payer_id = s.payer_id,
        encounter_class = s.encounter_class,
        encounter_code = s.encounter_code,
        total_claim_cost_cents = s.total_claim_cost_cents,
        payer_coverage_cents = s.payer_coverage_cents,
        row_hash = s.row_hash,
        source_batch_id = s.source_batch_id,
        source_file_sha256 = s.source_file_sha256,
        source_content_sha256 = s.source_content_sha256,
        loaded_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT
        (id, start_at, stop_at, patient_id, payer_id, encounter_class, encounter_code, total_claim_cost_cents, payer_coverage_cents, row_hash, source_batch_id, source_file_sha256, source_content_sha256, loaded_at)
    VALUES
        (s.id, s.start_at, s.stop_at, s.patient_id, s.payer_id, s.encounter_class, s.encounter_code, s.total_claim_cost_cents, s.payer_coverage_cents, s.row_hash, s.source_batch_id, s.source_file_sha256, s.source_content_sha256, CURRENT_TIMESTAMP());

    UPDATE BATCH_METRICS SET
        reconciled_rows = (SELECT COUNT(*) FROM TYPED_ENCOUNTERS s
            JOIN ENCOUNTERS t ON t.id = s.id WHERE EQUAL_NULL(t.id, s.id) AND EQUAL_NULL(t.start_at, s.start_at) AND EQUAL_NULL(t.stop_at, s.stop_at) AND EQUAL_NULL(t.patient_id, s.patient_id) AND EQUAL_NULL(t.payer_id, s.payer_id) AND EQUAL_NULL(t.encounter_class, s.encounter_class) AND EQUAL_NULL(t.encounter_code, s.encounter_code) AND EQUAL_NULL(t.total_claim_cost_cents, s.total_claim_cost_cents) AND EQUAL_NULL(t.payer_coverage_cents, s.payer_coverage_cents) AND EQUAL_NULL(t.row_hash, s.row_hash)),
        target_total_rows = (SELECT COUNT(*) FROM ENCOUNTERS)
    WHERE table_name = 'encounters';

    SELECT COUNT(*) INTO :failures FROM BATCH_METRICS
    WHERE source_rows <> reconciled_rows
       OR inserted + updated + unchanged <> source_rows;
    IF (failures <> 0) THEN
        RAISE target_error;
    END IF;

    INSERT INTO LOAD_RUNS
    SELECT :run_key, '20261026T235514_a3bfed17dc09', 'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c',
        'passed', :run_started, CURRENT_TIMESTAMP(),
        OBJECT_AGG(table_name, OBJECT_CONSTRUCT(
            'inserted', inserted, 'updated', updated, 'unchanged', unchanged,
            'source_rows', source_rows, 'reconciled_rows', reconciled_rows,
            'target_total_rows', target_total_rows)), NULL, NULL
    FROM BATCH_METRICS;
    COMMIT;
    transaction_open := FALSE;

    result_rows := (SELECT :run_key AS run_id, 'passed' AS status, *
        FROM BATCH_METRICS ORDER BY table_name);
    RETURN TABLE(result_rows);
EXCEPTION
    WHEN OTHER THEN
        IF (transaction_open) THEN
            ROLLBACK;
        END IF;
        INSERT INTO LOAD_RUNS VALUES (
            :run_key, '20261026T235514_a3bfed17dc09', 'a75ef822da0e3b6f62708a493f2144b428808ef723bbe51835c9b5a5b617ad9c', 'failed',
            :run_started, CURRENT_TIMESTAMP(), NULL, :SQLCODE, :SQLERRM
        );
        RAISE;
END;
$$;
