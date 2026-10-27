with expected as (
    select id as patient_id, birthdate, deathdate, gender, city, state
    from {{ source('healthcare_raw', 'patients') }}
), actual as (
    select patient_id, birthdate, deathdate, gender, city, state from {{ ref('dim_patient') }}
), missing_or_changed as (
    select * from expected except select * from actual
), extra_or_changed as (
    select * from actual except select * from expected
)
select * from missing_or_changed union all select * from extra_or_changed
