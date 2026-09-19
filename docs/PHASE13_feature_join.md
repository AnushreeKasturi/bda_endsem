# Phase 13 — Feature Join

**Status:** Complete. Join mechanism verified against the Spark API stub
(`leftOuterJoin`). Demo sentiment data cleaned up and expanded with real,
live-collected data — see "Demo-data cleanup" below.

---

## Demo-data cleanup (real work done in this phase)

The original demo sentiment sample only ever contained Kabbage and
Sifteo. A third company, Instructure, had been part of Phase 7's original
3-company live spike (see `docs/PHASE07_news_collection.md`) and was
referenced in Phase 8's docs, but its actual headline text was never
captured into the codebase — only its item count survived. That left a
dangling, unbacked reference rather than genuine data.

Rather than either fabricate Instructure headlines (which would violate
this project's "real, not synthetic" principle) or leave the stale
reference in place, two new real, eligible startups were confirmed and
collected live in this phase:

| Company | Confirmed eligible? | Funding | Label | Live yield | Accepted by NameMatcher |
|---|---|---|---|---|---|
| 1-800-DOCTORS | Yes (`grep` against `labeled_eligible_startups.csv`) | $1.75M | 1 (operating) | 3 items | **0** — all rejected, generic hospital-press noise with no real token overlap |
| Color Labs | Yes (`grep` confirmed) | $41M | 0 (closed) | 6 items (5 captured) | **4** — 1 rejected (unrelated obituary, 0/2 token overlap) |
| Yammer | Checked, **not eligible** | — | excluded (R2: `acquired` status) | — | — |

Yammer was a useful negative check: its absence from the eligible CSV
confirmed Phase 2's R2 exclusion rule (acquired startups excluded) is
working correctly on real data, not a grep mistake — verified both with
an anchored and an unanchored case-insensitive `grep`.

**1-800-DOCTORS is now the project's first genuine, real-data verification
of the `leftOuterJoin`'s `None` branch** — a real eligible startup, with
real financial features, whose real live news search returned real
results that were all correctly rejected, landing it on the zero-baseline
sentiment path. Previously this branch was only exercised abstractly (as
"the other 22,073 startups, not yet checked").

**Color Labs became the project's second positive-coverage company**,
alongside Kabbage. Its 4 accepted headlines also surfaced two genuine,
reportable lexicon limitations in `SentimentScorer.scala`:
- **No negation handling** — "$41 million *can't buy* success" scores
  Neutral (the positive term "success" and negative term "shutdown"
  cancel out) despite being an unambiguously negative headline.
- **Inflection gap** — "acquire" is absent from `PositiveTerms` (only
  "acquired"/"acquires"/"acquisition" are present), so it's silently
  skipped rather than scored.

Both are worth stating explicitly in the report's limitations section,
in the same spirit as Phase 8's Kabbage "tomato barons" false-positive
discussion — real, defensible tradeoffs of a transparent lexicon
approach, not bugs to hide.

## Demo-data cleanup, round 2 — Sifteo was never eligible

After the round-1 cleanup above, the first real run on live Spark
surfaced a genuine bug (not a demo-data quirk): `sentimentRDD` correctly
showed 2 companies (Kabbage, Sifteo), but the joined output only showed
**1** startup with real coverage, not 2.

Root cause: **Sifteo was acquired by 3D Robotics in July 2014.** Under
Phase 2's R2 rule, `acquired` status is excluded from
`labeled_eligible_startups.csv` entirely - confirmed with `grep -i
"sifteo"` returning nothing, the same signature as the Yammer check
below. Since `leftOuterJoin` only preserves rows keyed by the financial
(left) side, Sifteo's real, correctly-scored news data had nothing to
join onto and silently disappeared from `combinedRDD` - never even
reaching the `None`-vs-`Some` branch.

This does **not** invalidate Phases 9-10, which only tested name-matching
and sentiment scoring in isolation and never required eligibility.
Sifteo's data remains valid there (kept, with a note, in
`FeatureExtractor.scala`/`SentimentScorer.scala`). It does mean Sifteo
cannot correctly appear in this phase's full-population join demo.

**Pinterest** replaces it: grep-confirmed eligible ($1.3B funding,
label=1, never acquired - so no repeat of the Yammer/Sifteo trap). Its
live NewsCollector run returned 100 pre-cutoff items (a striking data
point on its own re: Phase 7's funding-correlates-with-coverage finding);
the first 5 (NewsCollector's spike-run display cap) were used, all
accepted by NameMatcher at confidence 1.0.

**A third real NameMatcher limitation, found via Pinterest:**
`isAmbiguousName`'s ≤4-character guard only catches *short* generic
names. "Pinterest" (9 chars) clears it easily, so every headline
containing the word gets full 1.0 confidence - including "42 Awesome and
Creative Pinterest Boards," a generic listicle using the product as a
common noun, not real company news. Worth stating alongside the
negation-handling and inflection-gap limitations from round 1.

The final real demo sample for Phase 13's join:

| Company | Eligible? | Funding | Label | Accepted headlines | Net sentiment |
|---|---|---|---|---|---|
| Kabbage | Yes | ~$50M+ | 1 | 2 | +0.5 (positive) |
| Color Labs | Yes | $41M | 0 | 4 | −0.5 (negative) |
| Pinterest | Yes | $1.3B | 1 | 5 | +0.4 (mildly positive) |
| 1-800-DOCTORS | Yes | $1.75M | 1 | 0 (real zero-coverage) | — |
| Sifteo | **No** (acquired, R2-excluded) | — | — | not used in join | — |
| Yammer | **No** (acquired, R2-excluded) | — | — | not checked further | — |

`NewsRDD.scala`, `FeatureExtractor.scala`, `SentimentScorer.scala`, and
`Aggregation.scala` were all updated again to match.

---

## The key design decision: `leftOuterJoin`, not `join`

Phase 12's `NewsRDD.scala` demonstrated plain `join()` — an **inner
join** — as a mechanism preview. That was appropriate for a small,
2-company preview, but it is the **wrong join type for the real
pipeline**: an inner join only keeps keys present in *both* RDDs. Since
Phase 7 found news coverage correlates strongly with funding size, most
of the 22,075 startups have no news coverage at all — an inner join here
would silently drop the vast majority of the population, which breaks
Experiments 1 and 2 (Financial-only), which need the **full** 22,075-
startup population, not just the small subset with news coverage.

`leftOuterJoin` keeps every row from the financial side (all 22,075
startups) and produces `None` on the sentiment side for any startup with
no match. Those `None` cases become a zero-baseline `SentimentFeatures`
via `FeatureExtractor.extractSentimentFeatures(permalink, List.empty,
cutoff)` — **reusing the exact same empty-case logic already
hand-verified in Phase 10's "no-coverage startup" test**, rather than
re-deriving or duplicating those zero values here.

## A small necessary change to `FinancialFeatures`

Adding `label: Int` to the case class (and populating it in
`FinancialFeatureExtractor`) — every downstream ML step needs the label
attached to its feature vector, and this was the natural point to add it
since `CombinedFeatures` needs it directly. Verified via stub-compile that
this addition doesn't break anything from Phase 11.

## Persistence

The joined `combinedRDD` is persisted because it directly feeds **both**
Phase 14 (Financial-only, which simply ignores the sentiment half of
`CombinedFeatures`) and Phase 15 (Financial+Sentiment, which uses both
halves) — exactly the "combined feature dataset reused for multiple
downstream pipelines" scenario the project brief specifically calls out
as needing persistence.

## Current scope — read this before running

Sentiment features currently only exist for the small real Kabbage /
Sifteo / Color Labs / 1-800-DOCTORS sample from Phases 7–13 (see
"Demo-data cleanup" above). **Full batch news collection across the
22,075 startups has not been run yet.** This means:

- `financialCount` will correctly show **22075**
- `sentimentRDD` (before the join) will show **3** — Kabbage, Color
  Labs, and Pinterest each have at least one accepted headline.
  1-800-DOCTORS is *not* in this count: all 3 of its candidate headlines
  were correctly rejected by NameMatcher, so it produces no entry in the
  News RDD at all — exactly like any of the other 22,072 startups with
  no accepted coverage. Sifteo is also not in this count — it was found
  to be genuinely ineligible (acquired, R2-excluded) and removed from
  this sample; see "Demo-data cleanup, round 2" above.
- After `leftOuterJoin`, `combinedCount` will still correctly show
  **22075** — proving the join mechanism preserves the full population
- Of those, only **3** will have `newsCount > 0`; the other **22072**
  will carry the zero-baseline sentiment features — including
  1-800-DOCTORS, whose zero-baseline is now backed by a real, verified
  live search rather than only a hypothetical case

This is expected and correct given the current data — **the point of this
run is to verify the join mechanism works correctly at full scale**, not
to produce meaningful sentiment numbers yet. Once batch news collection
runs (the next real task), these ratios will change to reflect real
coverage, and the same join code runs unchanged.

## What to run

```bash
cd ~/startup-survival-prediction/scala
sbt clean compile
sbt "runMain com.startupsurvival.spark.FeatureJoin" 2>&1 | tee /tmp/join_run.txt
grep -a "Financial features\|Sentiment features\|Combined features\|coverage\|baseline\|Expect\|label=" /tmp/join_run.txt
```

### Expected output

```
Financial features built for: 22075 startups (expect 22075)
Sentiment features built for: 3 startups (expect 3)
Combined features: 22075 startups
Startups with real news coverage: 3
Startups with zero-baseline sentiment (no news collected yet): 22072
```

If `combinedCount` comes back as anything other than 22075, the join is
behaving like an inner join, not a left outer join — stop and report the
exact number.

## Next

**Batch news collection.** This is now the genuinely necessary next step
before Phases 14–18 can produce meaningful results — everything currently
built (Phases 7–13) is correct and verified, but running on a real subset
of startups (not 2) is what turns the Financial+Sentiment experiments from
"mechanically correct" into "actually informative." We'll design the
batch collection approach (subset size, rate limiting, checkpointing
against partial failures) together as the next concrete task.
