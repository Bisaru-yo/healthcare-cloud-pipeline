with expected as (
    select encounter_month, count(*) as n from {{ ref('fct_encounter') }}
    group by encounter_month
), actual as (
    select encounter_month, sum(encounter_count) as n, sum(encounter_share) as share
    from {{ ref('mart_encounter_class_mix') }} group by encounter_month
)
select coalesce(e.encounter_month, a.encounter_month) as encounter_month
from expected e full outer join actual a on e.encounter_month = a.encounter_month
where e.n is null or a.n is null or e.n <> a.n or abs(a.share - 1) > 0.00001
