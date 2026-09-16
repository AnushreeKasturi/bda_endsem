# Phase 3 — Schema & Architecture Design

**Status:** Complete. Real column listing confirmed against Phase 2's
output — matched the draft exactly, 15/15 columns. `hive/schema.sql` and
`hive/analysis_queries.sql` finalized. `StartupRecord.scala` required no
changes.

---

## Why this phase exists

Phase 2 produced one canonical CSV. From here on, that data moves through
Hive, Scala, and Spark. If the schema is redefined ad hoc in each phase,
small mismatches (a `Double` where Scala expects an `Option[Double]`, a
column renamed halfway through) become debugging sessions instead of
one-line fixes. This phase fixes the schema and the data contracts once.

## Data flow (confirmed, does not change downstream)

```
labeled_eligible_startups.csv   (Phase 2 output, 22,075 rows)
        |
        v
HDFS  /startup-survival/processed/startups/
        |
        v
Hive external table  startups_eligible      <- SQL exploration (Phase 4)
        |
        v
Scala DataCleaner.scala                     <- parses into StartupRecord (Phase 6)
        |
        v
Spark RDD[(String, StartupRecord)]          <- keyed by permalink (Phase 12)
        |
        +---- join ----+
        |               |
        v               v
  Financial-only    News RDD -> SentimentFeatures
  feature vector    joined by permalink (Phase 13)
        |               |
        +-------+-------+
                v
  Financial+Sentiment feature vector
                v
        Spark MLlib (Phase 14-18)
```

**Fixed join key at every stage: `permalink`.** Set in Phase 2 (R5), applies
identically to the Hive table, the Scala case classes, and every Spark RDD.

## Case classes (see `scala/src/main/scala/com/startupsurvival/models/`)

- **`StartupRecord`** — one row of the Phase 2 output. `Option[_]` used for
  every field that Phase 1/2 showed can genuinely be missing (funding
  total, last funding date, etc.); non-`Option` used only for fields Phase 2
  guarantees are present and valid (`foundedAt`, `firstFundingAt`, `label`).
  A `require()` guard directly encodes the Phase 2 R4 leakage rule
  (`foundedAt` must precede the cutoff) so a violation fails loudly at
  construction time rather than silently propagating into a model.

- **`NewsRecord`** / **`ScoredNewsRecord`** / **`SentimentFeatures`** —
  the news pipeline's data contract, built ahead of Phases 7–10 so those
  phases have a fixed target shape to implement against, rather than
  inventing the shape mid-phase.

These are **draft** in the sense that field names may be adjusted once the
real Phase 2 column listing is confirmed (see checklist below) — but the
overall shape and the `Option`/leakage-guard design will not change.

## Hive schema — finalized

Confirmed real columns (from `data/processed/labeled_eligible_startups.csv`):
`permalink, name, homepage_url, category_list, funding_total_usd,
country_code, state_code, region, city, funding_rounds, founded_at,
first_funding_at, last_funding_at, label, prediction_cutoff` — 15 columns,
exact match to the draft. See `hive/schema.sql` for the full
`CREATE EXTERNAL TABLE` and built-in sanity-check queries, and
`hive/analysis_queries.sql` for exploratory SQL. Type mapping used:

| CSV column | Hive type | Notes |
|---|---|---|
| `permalink` | `STRING` | Hive table key |
| `name` | `STRING` | |
| text/category columns | `STRING` | nullable |
| `funding_total_usd` | `STRING` | kept as raw string in Hive; parsed to `DOUBLE` only in Scala (Phase 6), because the raw `"-"` placeholder is not valid Hive `DOUBLE` input |
| `funding_rounds` | `INT` | |
| `founded_at`, `first_funding_at`, `last_funding_at` | `DATE` | |
| `label` | `TINYINT` | 0/1 |
| `prediction_cutoff` | `DATE` | constant per Phase 2 |

Hive is queried for **exploratory SQL** (Phase 4) — aggregate checks,
category/country breakdowns — not for feature engineering itself, which
happens in Scala/Spark for syllabus-alignment reasons (Unit 2/3).

## Checklist — all complete

- [x] Real column listing from `labeled_eligible_startups.csv` confirmed
- [x] `hive/schema.sql` finalized against confirmed columns
- [x] `StartupRecord.scala` checked — no changes needed, exact match

## Next

**Phase 4 — Hadoop / HDFS / Hive setup.** Install/configure a single-node
HDFS, create the `/startup-survival/` directory structure, load
`labeled_eligible_startups.csv` into HDFS, create the Hive external table
using the now-finalized `hive/schema.sql`, and run the exploratory queries
in `hive/analysis_queries.sql`.
