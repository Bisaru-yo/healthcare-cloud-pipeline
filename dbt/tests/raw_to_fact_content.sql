-- Bidirectional content comparison catches dropped, altered and extra encounter keys.
with expected as (
    select id as encounter_id, patient_id, payer_id, start_at, stop_at,
        encounter_class, encounter_code, total_claim_cost_cents, payer_coverage_cents,
        row_hash, source_batch_id, source_file_sha256, source_content_sha256,
        loaded_at as raw_loaded_at
    from {{ source('healthcare_raw', 'encounters') }}
), actual as (
    select encounter_id, patient_id, payer_id, start_at, stop_at,
        encounter_class, encounter_code, total_claim_cost_cents, payer_coverage_cents,
        row_hash, source_batch_id, source_file_sha256, source_content_sha256,
        raw_loaded_at
    from {{ ref('fct_encounter') }}
), missing_or_changed as (
    select * from expected except select * from actual
), extra_or_changed as (
    select * from actual except select * from expected
)
select * from missing_or_changed
union all
select * from extra_or_changed
