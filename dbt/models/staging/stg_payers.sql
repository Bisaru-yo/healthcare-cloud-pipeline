select
    id as payer_id,
    name as payer_name,
    ownership,
    row_hash,
    source_batch_id,
    source_file_sha256,
    source_content_sha256,
    loaded_at as raw_loaded_at
from {{ source('healthcare_raw', 'payers') }}
