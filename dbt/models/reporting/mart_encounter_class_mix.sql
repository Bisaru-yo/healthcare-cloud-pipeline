select
    encounter_month,
    encounter_class,
    count(*) as encounter_count,
    count(distinct patient_id) as distinct_patient_count,
    round(cast(count(*) as number(38, 6)) /
        nullif(sum(count(*)) over (partition by encounter_month), 0), 6) as encounter_share
from {{ ref('fct_encounter') }}
group by encounter_month, encounter_class
