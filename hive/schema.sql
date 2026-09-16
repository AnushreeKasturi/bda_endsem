CREATE DATABASE IF NOT EXISTS startup_survival;

USE startup_survival;

DROP TABLE IF EXISTS startups_eligible;

CREATE EXTERNAL TABLE startups_eligible (
    permalink STRING,
    name STRING,
    homepage_url STRING,
    category_list STRING,
    funding_total_usd STRING,
    country_code STRING,
    state_code STRING,
    region STRING,
    city STRING,
    funding_rounds INT,
    founded_at DATE,
    first_funding_at DATE,
    last_funding_at DATE,
    label TINYINT,
    prediction_cutoff DATE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/startup-survival/processed/startups/'
TBLPROPERTIES ('skip.header.line.count'='1');

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
