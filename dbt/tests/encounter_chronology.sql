select encounter_id from {{ ref('fct_encounter') }}
where stop_at < start_at
