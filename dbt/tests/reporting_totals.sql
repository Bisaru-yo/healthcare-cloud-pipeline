with expected as (
    select count(*) as n, sum(total_claim_cost_cents) as cost,
        sum(payer_coverage_cents) as coverage from {{ ref('fct_encounter') }}
), reports as (
    select 'monthly_utilization' as report_name, sum(encounter_count) as n,
        sum(total_claim_cost_cents) as cost, sum(payer_coverage_cents) as coverage
    from {{ ref('mart_monthly_utilization') }}
    union all
    select 'payer_cost_coverage', sum(encounter_count), sum(total_claim_cost_cents),
        sum(payer_coverage_cents) from {{ ref('mart_payer_cost_coverage') }}
)
select r.report_name from reports r cross join expected e
where coalesce(r.n, 0) <> e.n or coalesce(r.cost, 0) <> coalesce(e.cost, 0)
    or coalesce(r.coverage, 0) <> coalesce(e.coverage, 0)
