-- Synthetic encounter-level amounts, not revenue or a payment ledger.
select
    e.encounter_month,
    e.payer_id,
    p.payer_name,
    count(*) as encounter_count,
    count(distinct e.patient_id) as distinct_patient_count,
    sum(e.total_claim_cost_cents) as total_claim_cost_cents,
    sum(e.payer_coverage_cents) as payer_coverage_cents,
    cast(sum(e.total_claim_cost_cents) / 100.0 as number(38, 2)) as total_claim_cost_amount,
    cast(sum(e.payer_coverage_cents) / 100.0 as number(38, 2)) as payer_coverage_amount,
    round(cast(sum(e.payer_coverage_cents) as number(38, 6)) /
        nullif(sum(e.total_claim_cost_cents), 0), 6) as coverage_to_cost_ratio
from {{ ref('fct_encounter') }} e
join {{ ref('dim_payer') }} p on e.payer_id = p.payer_id
group by e.encounter_month, e.payer_id, p.payer_name
