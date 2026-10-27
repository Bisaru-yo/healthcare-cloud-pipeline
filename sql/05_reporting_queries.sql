USE ROLE ACCOUNTADMIN;
USE WAREHOUSE HEALTHCARE_WH;
USE DATABASE HEALTHCARE;
USE SCHEMA ANALYTICS;

SELECT COUNT(*) AS encounters, SUM(total_claim_cost_cents) AS total_claim_cost_cents,
    SUM(payer_coverage_cents) AS payer_coverage_cents FROM FCT_ENCOUNTER;
SELECT * FROM MART_MONTHLY_UTILIZATION
ORDER BY encounter_month DESC, encounter_count DESC LIMIT 20;
SELECT * FROM MART_ENCOUNTER_CLASS_MIX
ORDER BY encounter_month DESC, encounter_count DESC LIMIT 20;
SELECT * FROM MART_PAYER_COST_COVERAGE
ORDER BY encounter_month DESC, total_claim_cost_cents DESC LIMIT 20;
