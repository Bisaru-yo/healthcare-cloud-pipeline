select patient_id from {{ ref('dim_patient') }}
where deathdate < birthdate
