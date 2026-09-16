-- =============================================================================
-- Phase 4 - Exploratory Hive Queries
-- =============================================================================
-- Run after hive/schema.sql has created and loaded startups_eligible.
-- These queries demonstrate Hive's role in the architecture: SQL-based
-- exploration of the structured side of the data (Unit 1 syllabus
-- alignment), separate from the Scala/Spark feature-engineering pipeline.
-- =============================================================================

USE startup_survival;

-- -----------------------------------------------------------------------------
-- 1. Survival rate by category (top 15 by volume)
-- -----------------------------------------------------------------------------
SELECT
    category_list,
    COUNT(*)                                   AS total_startups,
    SUM(label)                                  AS operating_count,
    COUNT(*) - SUM(label)                       AS closed_count,
    ROUND(SUM(label) / COUNT(*) * 100, 2)       AS survival_rate_pct
FROM startups_eligible
WHERE category_list IS NOT NULL
GROUP BY category_list
ORDER BY total_startups DESC
LIMIT 15;

-- -----------------------------------------------------------------------------
-- 2. Survival rate by country (top 15 by volume)
-- -----------------------------------------------------------------------------
SELECT
    country_code,
    COUNT(*)                                   AS total_startups,
    SUM(label)                                  AS operating_count,
    COUNT(*) - SUM(label)                       AS closed_count,
    ROUND(SUM(label) / COUNT(*) * 100, 2)       AS survival_rate_pct
FROM startups_eligible
WHERE country_code IS NOT NULL
GROUP BY country_code
ORDER BY total_startups DESC
LIMIT 15;

-- -----------------------------------------------------------------------------
-- 3. Funding rounds vs survival - does more funding activity correlate
--    with survival in this eligible (pre-cutoff) population?
-- -----------------------------------------------------------------------------
SELECT
    funding_rounds,
    COUNT(*)                                   AS total_startups,
    ROUND(AVG(label) * 100, 2)                  AS survival_rate_pct
FROM startups_eligible
GROUP BY funding_rounds
ORDER BY funding_rounds;

-- -----------------------------------------------------------------------------
-- 4. Founding year distribution (should be concentrated pre-2013 by
--    construction - a Phase 2 R4 sanity check re-run at the Hive layer)
-- -----------------------------------------------------------------------------
SELECT
    YEAR(founded_at) AS founding_year,
    COUNT(*)         AS total_startups,
    ROUND(AVG(label) * 100, 2) AS survival_rate_pct
FROM startups_eligible
GROUP BY YEAR(founded_at)
ORDER BY founding_year;

-- -----------------------------------------------------------------------------
-- 5. Missing/unparseable funding_total_usd rate (expect ~19.3%, per Phase 1 S6)
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                                    AS total_rows,
    SUM(CASE WHEN funding_total_usd RLIKE '^[0-9]+$'
             THEN 0 ELSE 1 END)                                  AS unparseable_funding,
    ROUND(
        SUM(CASE WHEN funding_total_usd RLIKE '^[0-9]+$'
                 THEN 0 ELSE 1 END) / COUNT(*) * 100, 2
    )                                                            AS unparseable_pct
FROM startups_eligible;

-- -----------------------------------------------------------------------------
-- 6. last_funding_at nullness rate (expect a substantial share nulled by
--    Phase 2 R4 as post-cutoff/unsafe - not an error, confirms the rule ran)
-- -----------------------------------------------------------------------------
SELECT
    COUNT(*)                                        AS total_rows,
    SUM(CASE WHEN last_funding_at IS NULL
             THEN 1 ELSE 0 END)                       AS null_last_funding,
    ROUND(
        SUM(CASE WHEN last_funding_at IS NULL THEN 1 ELSE 0 END)
        / COUNT(*) * 100, 2
    )                                                  AS null_pct
FROM startups_eligible;

-- -----------------------------------------------------------------------------
-- 7. Time from founding to first funding (days) - sanity distribution;
--    negative values here would flag the "funding before founding" data
--    quirk noted in docs/PHASE02_labels_and_cutoff.md
-- -----------------------------------------------------------------------------
SELECT
    permalink,
    name,
    founded_at,
    first_funding_at,
    DATEDIFF(first_funding_at, founded_at) AS days_to_first_funding
FROM startups_eligible
WHERE DATEDIFF(first_funding_at, founded_at) < 0
ORDER BY days_to_first_funding ASC
LIMIT 20;
-- Non-empty result is expected and documented, not a bug - see
-- docs/PHASE02_labels_and_cutoff.md for the known data-quality note.
