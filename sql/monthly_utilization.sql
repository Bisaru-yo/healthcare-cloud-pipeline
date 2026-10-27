-- Executed locally by pipeline/storage.py, against the normalized source projection.
-- Amounts are integer cents. Unique patients are distinct within each group only.
SELECT
    substr(e.start, 1, 7) AS encounter_month,
    e.encounterclass,
    p.id AS payer_id,
    p.name AS payer_name,
    COUNT(*) AS encounter_count,
    COUNT(DISTINCT e.patient) AS unique_patients,
    SUM(e.total_claim_cost_cents) AS total_claim_cost_cents,
    SUM(e.payer_coverage_cents) AS payer_coverage_cents
FROM source_encounters e
JOIN source_payers p ON p.id = e.payer
GROUP BY substr(e.start, 1, 7), e.encounterclass, p.id, p.name
ORDER BY encounter_month, e.encounterclass, payer_id;
