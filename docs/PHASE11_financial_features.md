# Phase 11 — Financial/Startup Feature Engineering

**Status:** Complete. `FinancialFeatureExtractor.scala` compiled and
tested against three cases specifically designed to prove the leakage
guard actually intercepts leaky values, not just that formulas compute
correctly in the easy case.

---

## The leakage risk this phase found and fixed

`StartupRecord.fundingTotalUsd` and `.fundingRounds` are **lifetime
totals** from the raw Crunchbase snapshot (which extends to ~2014/2015 —
see `docs/PHASE01_dataset_analysis.md`), not pre-cutoff-only figures.
Phase 2's R4 rule nulled `lastFundingAt` whenever it fell on/after the
cutoff, but never touched `fundingTotalUsd` or `fundingRounds`
themselves. Used directly, either field could silently leak post-cutoff
funding activity into a "financial-only" feature set — undermining the
entire leakage-prevention design the project is built around.

### The fix

`lastFundingAt` is a **maximum** — the date of a startup's most recent
funding event. If it is defined (survived Phase 2's nulling, meaning it
is confirmed `< cutoff`), then by definition every funding event for that
startup happened before the cutoff, since none can exceed the maximum.
**Only in that case** are `totalFundingUsd` and `fundingRoundCount`
treated as usable; otherwise both are set to `None`, explicitly marked
unusable via the `fundingDataLeakageSafe` flag rather than silently
leaked.

## Features computed

| Feature | Formula | Leakage-guarded? |
|---|---|---|
| `startupAgeYears` | (cutoff − foundedAt) / 365.25 | N/A — `foundedAt` is already guaranteed pre-cutoff by Phase 2 R4 |
| `totalFundingUsd` | raw lifetime total, **only if `lastFundingAt` defined** | Yes |
| `fundingRoundCount` | raw lifetime count, **only if `lastFundingAt` defined** | Yes |
| `averageRoundSizeUsd` | total / roundCount | Inherits guard from both inputs |
| `timeSinceLastFundingDays` | cutoff − lastFundingAt | N/A — only defined when `lastFundingAt` itself is defined |
| `fundingFrequencyPerYear` | roundCount / ageYears | Inherits guard from roundCount |
| `hasFundingAmountRecorded` | whether the raw record had any funding amount (not the `-` placeholder) | Diagnostic only, deliberately independent of the leakage guard |
| `fundingDataLeakageSafe` | whether `lastFundingAt` is defined | The guard flag itself, exposed for transparency |
| `categoryList`, `countryCode` | passthrough | For categorical encoding in Phase 14/15 |

## Features NOT computed, and why

- **`investor_count` / `unique_investor_count`** — require per-round
  investor data, not present in the primary dataset. Available only via
  the optional `chhinna` Crunchbase enrichment flagged (and deliberately
  deferred for scope) in Phase 1.
- **`largest_round`** — requires per-round amounts; only an aggregate
  lifetime total and round count are available, no breakdown.

Both are stated here explicitly rather than fabricated as placeholder
values — a real dataset limitation, not an oversight.

## Verification — proving the guard actually fires

Three constructed `StartupRecord` cases:

| Case | `lastFundingAt` | Raw `fundingTotalUsd` | Output `totalFundingUsd` |
|---|---|---|---|
| A — leakage-safe | `Some(2012-06-01)` | `Some(5,000,000)` | **`Some(5,000,000)`** — used |
| B — NOT leakage-safe | `None` | `Some(50,000,000)` | **`None`** — correctly blocked |
| C — no funding recorded | `Some(2011-06-01)` | `None` | `None` (no data, not a guard block) |

**Case B is the critical proof.** The input record has a real, populated
`fundingTotalUsd` of $50M — but because `lastFundingAt` is `None` (meaning
we cannot confirm all of that startup's funding activity happened
pre-cutoff), the output correctly nulls it out. This demonstrates the
guard actively intercepting a genuinely leaky value, not just correctly
computing the easy case.

All numeric outputs were hand-verified: Case A's `averageRoundSizeUsd =
5,000,000 / 3 = 1,666,666.67` ✓; Case C's `fundingFrequencyPerYear = 1 /
2.001 ≈ 0.4997` ✓; Case C correctly shows `hasFundingAmountRecorded=false`
while `fundingDataLeakageSafe=true` simultaneously, confirming the two
flags are independent as designed (a startup can have no recorded amount
while still having a trustworthy, pre-cutoff-only round count).

## What this means for Phase 14/15

When building the Financial-only and Financial+Sentiment feature vectors,
expect a meaningful share of startups to have `totalFundingUsd = None`
and `fundingRoundCount = None` — not just from missing data (Phase 1's
~19.3%/15.2% figures) but now also from the leakage guard itself. The
exact combined rate should be measured directly against the real
22,075-row dataset before finalizing the missing-value handling strategy
in Phase 14 — don't assume the earlier missing-data percentage still
applies once this guard is active.

## Next

**Phase 12 — Spark RDD pipeline.** Build `StartupRDD.scala` and
`NewsRDD.scala`: load `StartupRecord`/`FinancialFeatures` and
`SentimentFeatures` as key/value RDDs keyed by `permalink`, demonstrating
the syllabus Unit 3 concepts (transformations, actions, lazy evaluation,
partitions, persistence) on the actual feature data built in Phases 6-11.
