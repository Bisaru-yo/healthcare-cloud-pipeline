select 'monthly_utilization' as report_name from {{ ref('mart_monthly_utilization') }}
group by encounter_month, encounter_class, payer_id having count(*) > 1
union all
select 'encounter_class_mix' from {{ ref('mart_encounter_class_mix') }}
group by encounter_month, encounter_class having count(*) > 1
union all
select 'payer_cost_coverage' from {{ ref('mart_payer_cost_coverage') }}
group by encounter_month, payer_id having count(*) > 1
