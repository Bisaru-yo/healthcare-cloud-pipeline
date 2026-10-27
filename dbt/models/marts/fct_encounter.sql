-- One row per encounter UUID, not one adjudicated claim line.
-- No dimension join or deduplication can hide a source orphan/duplicate.
select
    encounter_id,
    patient_id,
    payer_id,
    start_at,
    stop_at,
    cast(start_at as date) as encounter_date,
    cast(date_trunc('month', start_at) as date) as encounter_month,
    encounter_class,
    encounter_code,
    total_claim_cost_cents,
    payer_coverage_cents,
    cast(total_claim_cost_cents / 100.0 as number(38, 2)) as total_claim_cost_amount,
    cast(payer_coverage_cents / 100.0 as number(38, 2)) as payer_coverage_amount,
    row_hash,
    source_batch_id,
    source_file_sha256,
    source_content_sha256,
    raw_loaded_at
from {{ ref('stg_encounters') }}
