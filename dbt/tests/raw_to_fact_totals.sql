with source_totals as (
    select count(*) as n, sum(total_claim_cost_cents) as cost,
        sum(payer_coverage_cents) as coverage
    from {{ source('healthcare_raw', 'encounters') }}
), fact_totals as (
    select count(*) as n, sum(total_claim_cost_cents) as cost,
        sum(payer_coverage_cents) as coverage from {{ ref('fct_encounter') }}
)
select 'raw_to_fact' as check_name from source_totals s cross join fact_totals f
where s.n = 0 or s.n <> f.n
    or coalesce(s.cost, 0) <> coalesce(f.cost, 0)
    or coalesce(s.coverage, 0) <> coalesce(f.coverage, 0)
