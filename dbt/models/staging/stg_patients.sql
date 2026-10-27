select
    id as patient_id,
    birthdate,
    deathdate,
    gender,
    city,
    state,
    row_hash,
    source_batch_id,
    source_file_sha256,
    source_content_sha256,
    loaded_at as raw_loaded_at
from {{ source('healthcare_raw', 'patients') }}
