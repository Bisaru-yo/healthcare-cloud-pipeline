-- One current row per synthetic patient (type 1 dimension).
select * from {{ ref('stg_patients') }}
