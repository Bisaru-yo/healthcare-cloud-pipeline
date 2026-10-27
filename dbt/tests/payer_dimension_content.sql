with expected as (
    select id as payer_id, name as payer_name, ownership from {{ source('healthcare_raw', 'payers') }}
), actual as (
    select payer_id, payer_name, ownership from {{ ref('dim_payer') }}
), missing_or_changed as (
    select * from expected except select * from actual
), extra_or_changed as (
    select * from actual except select * from expected
)
select * from missing_or_changed union all select * from extra_or_changed
