# Phase 2 — Label Design & Prediction Cutoff

**Status:** Rules finalized and verified against a synthetic fixture.
Awaiting a run against the real dataset before Phase 3.

---

## Why this phase exists as its own step

Phase 1 characterised the raw data. Phase 2 turns those findings into a
**single, fixed set of rules** for two things every later phase depends on:

1. How raw `status` values become the binary label
2. Which rows/fields count as "known before the cutoff" (safe to use as a
   feature) versus "only known after" (label-only)

Writing this once, here, and re-implementing it identically in Scala
(Phase 6) prevents three independent, silently-diverging versions of the
same logic appearing in cleaning, feature engineering, and dataset
assembly.

---

## R1 — Prediction cutoff

```
T = 2013-01-01
```

Chosen deliberately over a later cutoff (e.g. 2014-01-01) that would yield
more rows. Reasoning: a later cutoff shrinks the post-cutoff observation
window in which a startup's eventual closure would actually get recorded in
the snapshot. That is a **right-censoring** risk — startups that would go on
to close might still be recorded "operating" simply because the closure
hadn't happened yet relative to a later T. This would quietly bias the
"operating" class toward startups that *hadn't failed yet* rather than
startups that were genuinely more resilient. Preserving label reliability
was judged more valuable than the marginal row-count gain from moving T
later — see chat decision log for the full comparison.

## R2 — Status → label mapping

| Raw `status` | Label | Justification |
|---|---|---|
| `closed` | **0** | Unambiguous failure |
| `operating` | **1** | Unambiguous survival |
| `acquired` | excluded | Cannot distinguish acqui-hire (disguised failure) from strategic exit (success) using this dataset alone |
| `ipo` | excluded | ~2% of rows; structurally different (older, larger) population; would inflate the majority class with easy cases |
| null / other | excluded | No usable label |

## R3 — Date validity window

Phase 1 found corrupted date values in the raw data (`1015-01-30`,
`2914-01-01`, etc — almost certainly typos of 2015/2014). Rule:

```
valid date range = [1950-01-01, 2016-12-31]
```

Any `founded_at`, `first_funding_at`, or `last_funding_at` falling outside
this range is treated as **missing (NaT)**, never as its literal corrupted
value. The exact count of values affected is printed by the Phase 2 script
and must be quoted in the report — not silently absorbed.

## R4 — Feature eligibility (the leakage guard)

A row is **eligible for modelling** only if all of the following hold:

```
status ∈ {closed, operating}
founded_at is valid AND founded_at < T
first_funding_at is valid AND first_funding_at < T
```

For eligible rows, `last_funding_at` is usable as a feature **only if** it
is also valid and `< T`. If not, the row is **kept** but that single field
is nulled — a startup isn't dropped from the dataset just because its most
recent funding event happened to fall after the cutoff; that event is simply
not treated as known-at-prediction-time.

The same `< T` rule governs later phases directly:

- **Phase 7–10 (news/sentiment):** any headline used for sentiment features
  must have `publication_date < T`. Articles published on or after T are
  discarded before feature computation, not just before display.
- **Phase 11 (financial features):** any feature derived from a funding
  event must check that event's own date against T individually — total
  funding as of T is not the same as total funding in the full dataset.

## R5 — Deduplication / join key

`permalink` is canonical. Phase 1 found 0 duplicate permalinks against 330
case-insensitive duplicate *names* — so `name` is never used as a dedup or
join key; only `permalink` is.

---

## Reference implementation

`scripts/label_and_cutoff_design.py` implements R1–R5 exactly as written
above and is the source of truth to check the Scala `DataCleaner.scala`
(Phase 6) against. If Phase 6's row counts ever disagree with this script's
output on the same input, that is a bug — the two must match exactly.

### Run

```bash
python scripts/label_and_cutoff_design.py \
    data/raw/big_startup_secsees_dataset.csv \
    data/processed/labeled_eligible_startups.csv \
    | tee results/phase2_report.txt
```

### Output

`data/processed/labeled_eligible_startups.csv` — the canonical eligible,
labeled dataset. Columns: `permalink`, `name`, all original non-date/status
columns, cleaned `founded_at` / `first_funding_at` / `last_funding_at`,
`label` (0/1), `prediction_cutoff`.

This file — not the raw CSV — is the input to every phase from here on.

### Verified behaviour (synthetic fixture, 3,000 rows)

Ran successfully end to end: dedup check, date corruption nulling (740/591
corrupted values detected and nulled rather than trusted), status mapping
(328 acquired/ipo/other rows excluded), temporal filter (2,672 → 800
eligible), and `last_funding_at` post-cutoff nulling (631/800 = 78.9%,
expected given the fixture's random post-2013 dates) all matched hand
verification. Output CSV columns and dtypes confirmed correct.

**This confirms the script works. It has not yet been run against the real
dataset — that is the next action.**

---

## Checklist before Phase 3

- [ ] Script run against `data/raw/big_startup_secsees_dataset.csv` (the
      real file, not the synthetic fixture used for verification)
- [ ] Final eligible row count recorded (expect roughly in the same range as
      Phase 1 §8's estimate of ~22,120, though R3's date-corruption nulling
      may shift this slightly — that's expected and fine)
- [ ] Final label balance recorded
- [ ] `results/phase2_report.txt` saved
- [ ] `data/processed/labeled_eligible_startups.csv` exists and spot-checked
      (open a few rows; confirm `label` is 0/1 and dates look sane)

## Next

**Phase 3 — Schema & architecture design.** Freeze the exact Hive table
schema and the Scala case classes, using the real column names and dtypes
confirmed by this phase's output — not assumptions.
