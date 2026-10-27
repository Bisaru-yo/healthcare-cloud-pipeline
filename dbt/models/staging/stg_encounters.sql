select
    id as encounter_id,
    patient_id,
    payer_id,
    start_at,
    stop_at,
    encounter_class,
    encounter_code,
    total_claim_cost_cents,
    payer_coverage_cents,
    row_hash,
    source_batch_id,
    source_file_sha256,
    source_content_sha256,
    loaded_at as raw_loaded_at
from {{ source('healthcare_raw', 'encounters') }}
