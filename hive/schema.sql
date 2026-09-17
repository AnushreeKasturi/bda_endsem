CREATE DATABASE IF NOT EXISTS startup_survival;

USE startup_survival;

DROP VIEW IF EXISTS startups_eligible;
DROP TABLE IF EXISTS startups_eligible_raw;

CREATE EXTERNAL TABLE startups_eligible_raw (
    permalink STRING,
    name STRING,
    homepage_url STRING,
    category_list STRING,
    funding_total_usd STRING,
    country_code STRING,
    state_code STRING,
    region STRING,
    city STRING,
    funding_rounds STRING,
    founded_at STRING,
    first_funding_at STRING,
    last_funding_at STRING,
    label STRING,
    prediction_cutoff STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION '/startup-survival/processed/startups/'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE VIEW startups_eligible AS
SELECT
    permalink,
    name,
    homepage_url,
    category_list,
    funding_total_usd,
    country_code,
    state_code,
    region,
    city,
    CAST(funding_rounds AS INT) AS funding_rounds,
    CAST(founded_at AS DATE) AS founded_at,
    CAST(first_funding_at AS DATE) AS first_funding_at,
    CAST(last_funding_at AS DATE) AS last_funding_at,
    CAST(label AS TINYINT) AS label,
    CAST(prediction_cutoff AS DATE) AS prediction_cutoff
FROM startups_eligible_raw;

SELECT COUNT(*) AS total_rows FROM startups_eligible;

SELECT label, COUNT(*) AS n
FROM startups_eligible
GROUP BY label
ORDER BY label;

SELECT COUNT(*) AS null_permalinks
FROM startups_eligible
WHERE permalink IS NULL;

SELECT permalink, COUNT(*) AS c
FROM startups_eligible
GROUP BY permalink
HAVING COUNT(*) > 1;

SELECT COUNT(*) AS leakage_violations
FROM startups_eligible
WHERE founded_at >= prediction_cutoff;
