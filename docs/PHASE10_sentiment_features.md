# Phase 10 — Sentiment Feature Engineering

**Status:** Complete. `FeatureExtractor.scala` compiled and tested against
a genuine cross-phase integration case — real dates from Phase 7, real
match-filtering results from Phase 8, real sentiment scores from Phase 9 —
with every output value hand-verified against the underlying data.

---

## Feature definitions (for the report's Feature Engineering section)

All 11 fields of `SentimentFeatures` (defined in Phase 3):

| Feature | Formula | Reasoning |
|---|---|---|
| `newsCount` | Count of eligible (pre-cutoff, matched) articles | Base coverage volume |
| `positiveNewsCount` / `negativeNewsCount` / `neutralNewsCount` | Count by `sentimentClass` | Raw sentiment distribution |
| `positiveRatio` / `negativeRatio` | count / `newsCount` | Normalizes for volume — distinguishes "1 of 1 positive" from "1 of 50 positive" |
| `averageSentiment` | mean of `sentimentScore` across eligible articles | Overall tone, captures *strength* not just direction |
| `recentSentiment` | `sentimentScore` of the single most recent pre-cutoff article | Distinct from the average — recent tone shift is a different signal than historical average |
| `sentimentTrend` | avg(chronologically later half) − avg(earlier half) | Positive = improving coverage over time; negative = declining |
| `sentimentVolatility` | sample standard deviation of `sentimentScore` | Captures inconsistency/swings in coverage, independent of average |
| `newsVolumeTrend` | count in most-recent 12 months of the lookback − count in the earlier 12 months | Matches `NewsCollector`'s 24-month lookback window (Phase 7); positive = coverage accelerating toward the cutoff |

**Fields requiring ≥2 articles** (`sentimentTrend`, `sentimentVolatility`) return `None` for startups with 0 or 1 article — a trend or a standard deviation is mathematically undefined with fewer than 2 points, and returning `None` rather than `0.0` avoids silently implying "no trend" when the real answer is "not computable."

**`newsVolumeTrend` is computed whenever any news exists** (≥1 article) — even a single-bucket result (all coverage in one half of the window, none in the other) is meaningful signal, not missing data.

## Leakage guard, re-enforced

`extractSentimentFeatures` re-applies the `publicationDate < cutoff` filter
even though `NewsCollector` (Phase 7) already filters at collection time.
This matches the project's established defensive pattern — see
`StartupRecord`'s `require()` guard from Phase 3. A single missed filter
anywhere in a multi-stage pipeline would silently reintroduce the exact
temporal leakage the whole project exists to prevent; re-checking cheaply
at each stage is worth the redundancy.

## Verification — a genuine cross-phase integration test

Rather than synthetic data, the test in `FeatureExtractor.main` uses:
- **Real dates** captured live from Phase 7's Google News RSS run
- **Only the articles Phase 8's `NameMatcher` actually accepted** (2 of 5
  for Kabbage, 4 of 5 for Sifteo — see `docs/PHASE08_name_matching.md`)
- **Real scores** from Phase 9's `SentimentScorer`

This means the test genuinely exercises Phases 7 through 10 together, not
just Phase 10 in isolation.

### Kabbage (2 real accepted articles)

| Article | Date | Score |
|---|---|---|
| "Kabbage Raises Some Serious Cabbage..." | 2012-09-19 | +1 (Positive — "raises") |
| "Kabbage: The Merchant Cash Advance..." | 2011-08-23 | 0 (Neutral) |

Result (hand-verified):
```
newsCount=2, positive=1, negative=0, neutral=1
positiveRatio=0.5, negativeRatio=0.0, averageSentiment=0.5
recentSentiment=1.0   (the 2012 article, the more recent one, scored +1)
sentimentTrend=+1.0   (2012 avg [1.0] minus 2011 avg [0.0] = improving)
sentimentVolatility=0.707  (sample stdev of [0, 1])
newsVolumeTrend=0.0   (1 article in each 12-month half of the window)
```

### Sifteo (4 real accepted articles)

All 4 articles scored Neutral (0) — consistent with Phase 9's finding
that descriptive product-announcement headlines often contain no
lexicon-matched sentiment terms at all.

Result (hand-verified): `newsCount=4, all counts/ratios reflect 100%
neutral, averageSentiment=0.0, recentSentiment=0.0, sentimentTrend=0.0,
sentimentVolatility=0.0, newsVolumeTrend=0.0` (2 articles fall in each
12-month half of the window).

**This is a legitimate, expected result worth discussing in the report**:
a meaningful share of real startup news coverage will produce entirely
neutral sentiment features under this lexicon, particularly for
product-launch-style coverage rather than funding/financial news. This
doesn't invalidate the feature set — it means `newsCount` and
`newsVolumeTrend` (which don't depend on sentiment direction) may carry
signal even when the sentiment-direction features don't, for startups
with mostly-neutral coverage.

### No-coverage case

A startup with zero eligible articles correctly returns a well-defined
zero/`None` baseline rather than an error — legitimate, expected data
given Phase 7's finding that most smaller startups will have sparse or no
retrievable coverage.

## Next

**Phase 11 — Financial/startup feature engineering.** Build the Group A
(structured) feature set from `StartupRecord` — `startup_age`,
`total_funding`, `funding_round_count`, `time_since_last_funding`, etc. —
completing the two ingredient sets (`StartupRecord` + `SentimentFeatures`)
that Phase 13 will join by `permalink`.
