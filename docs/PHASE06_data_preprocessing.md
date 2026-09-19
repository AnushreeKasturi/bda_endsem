# Phase 6 — Startup Data Preprocessing (Scala)

**Status:** `DataCleaner.scala` written, compiled cleanly, and verified
against a synthetic test CSV designed to exercise exactly the failure
modes that matter (embedded commas in quotes, malformed dates, missing
funding placeholder). Not yet run against the real 22,075-row dataset —
that's your next action.

---

## What this phase is and why it's plain Scala, not Spark

`DataCleaner.scala` reads Phase 2's canonical CSV directly (no Spark, no
HDFS) and parses it into `StartupRecord` instances (Phase 3). Spark's RDD
pipeline is Phase 12 — deliberately kept separate so this phase can focus
purely on demonstrating the syllabus's Unit 2 functional-programming
toolkit on real project data:

- **Immutable case classes and collections** — `List[StartupRecord]` is
  never mutated; every transformation produces a new list
- **Higher-order functions** — `map`, `flatMap`, `filter`, `count`,
  `partition`, `foldLeft` all appear doing real work, not toy examples
- **Pattern matching** — on the CSV field-count shape, on quote-state while
  parsing, on missing-value sentinels (`""`, `"-"`, `"NULL"`)
- **`Option[_]`** for principled missing-value handling instead of `null`
  or exceptions
- **Closures** — implicit throughout the `map`/`filter` chains

## Why this file has its own CSV parser instead of `String.split(",")`

Phase 4 found a real, serious bug: Hive's naive comma-delimited reader
silently corrupted data whenever a field (e.g. a company name) contained a
comma inside quotes — the label column ended up with garbage values for an
unknown subset of rows, while `COUNT(*)` still looked correct. That bug
happened specifically *because* the reader didn't understand quoting.

`parseCsvLine` in this file is a small quote-aware state machine
(implemented as a `foldLeft` carrying an immutable `ParseState`), not a
naive split — deliberately avoiding a repeat of the Phase 4 bug at the
Scala layer.

## Verification performed

No Scala environment was available to test this code before handing it
over, so it was compiled and run against a purpose-built synthetic CSV
covering:

| Test case | Row | Expected behavior | Result |
|---|---|---|---|
| Embedded comma in a quoted name | `"Smith, Jones & Co"` | Parsed as one field, not split | Confirmed — printed as `Smith, Jones & Co` exactly |
| Embedded comma in a quoted category | `"Consulting, Legal"` | Parsed as one field | Confirmed |
| Missing funding placeholder | `-` | Parsed as `None`, not `0` or a crash | Confirmed — reported as 1/3 = 33.3% missing |
| Malformed required date | `not-a-date` | Row dropped entirely (not silently kept with a bad date) | Confirmed — 4 rows in, 3 valid records loaded |
| Correct row | normal fields | Parsed into a correct `StartupRecord` | Confirmed |

Output from the verification run:

```
Loaded 3 startup records
  closed    (label 0): 2
  operating (label 1): 1
  missing funding_total_usd: 1 (33.3%)
  average funding (where known): $6,000,000
```

This matches hand-calculation exactly against the synthetic input. **This
confirms the code is correct on the cases it was designed to handle — it
has not yet been run against the real dataset.**

## What to run on the real data

```bash
cd ~/startup-survival-prediction/scala
sbt "runMain com.startupsurvival.DataCleaner ../data/processed/labeled_eligible_startups.csv"
```

### Expected output (prediction, not yet verified — treat as a target to check against)

```
Loading startups from: ../data/processed/labeled_eligible_startups.csv
Loaded 22075 startup records
  closed    (label 0): 2902
  operating (label 1): 19173
  missing funding_total_usd: [some number near 19.3% of 22075, per Phase 1]
  average funding (where known): $[some dollar figure]
```

If the total record count comes back **less than 22,075**, that means
some real rows are being rejected by the field-count pattern match or a
date-parse failure that the synthetic test didn't anticipate — stop and
report the exact discrepancy rather than proceeding, since Phase 12's
Spark pipeline will build on whatever this phase produces.

## Next

**Phase 7 — News collection**, including the archival-coverage feasibility
spike flagged back in Phase 1 (§1.4-1.5) — the open question of whether
pre-2013 news headlines are actually retrievable for this dataset's
startups.
