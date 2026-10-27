-- Distinct patients are counted within each group and cannot be summed across groups.
select
    e.encounter_month,
    e.encounter_class,
    e.payer_id,
    p.payer_name,
    count(*) as encounter_count,
    count(distinct e.patient_id) as distinct_patient_count,
    sum(e.total_claim_cost_cents) as total_claim_cost_cents,
    sum(e.payer_coverage_cents) as payer_coverage_cents,
    cast(sum(e.total_claim_cost_cents) / 100.0 as number(38, 2)) as total_claim_cost_amount,
    cast(sum(e.payer_coverage_cents) / 100.0 as number(38, 2)) as payer_coverage_amount
from {{ ref('fct_encounter') }} e
join {{ ref('dim_payer') }} p on e.payer_id = p.payer_id
group by e.encounter_month, e.encounter_class, e.payer_id, p.payer_name
