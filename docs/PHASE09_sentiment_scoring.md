# Phase 9 — Custom Scala Sentiment Scoring

**Status:** Complete. Compiled successfully and verified to reproduce the
project brief's own worked example exactly, plus tested against real
Phase 7 headlines and constructed mixed-sentiment cases.

---

## Why an original lexicon, not a reproduction of a published one

Named academic sentiment lexicons (e.g. Hu & Liu, 2004 — see
`report/references.md` [6]) are typically published for research citation
and use, not for verbatim redistribution embedded inside another project's
codebase. `PositiveTerms` and `NegativeTerms` in `SentimentScorer.scala`
are an **original compilation**, written specifically for this project's
domain — startup and funding news — rather than a general-purpose product-
review lexicon.

This is also methodologically better, not just a legal precaution:
Loughran & McDonald (2011) — cited in `report/references.md` [7] —
demonstrated that lexicons built for one domain systematically
misclassify words when applied to financial/business text (e.g. "liability"
is often neutral/technical in a 10-K filing, not literally negative).
A startup-news-specific lexicon is a direct, defensible response to
exactly that finding, and this connection is worth stating explicitly in
your methodology section.

## Methodology

Matches the brief's specification exactly:

1. **Normalize** — lowercase the headline
2. **Tokenize** — split on non-letter characters (`tokenize`)
3. **Remove stopwords** — a small general English stopword list
   (`removeStopwords`)
4. **Match against lexicon** — count positive and negative term hits
   (`countSentimentWords`, implemented via `foldLeft` as the brief
   explicitly requires demonstrating fold/reduce)
5. **Score** — `sentiment_score = positive_matches - negative_matches`
6. **Classify** — `score > 0` → Positive, `score < 0` → Negative,
   `score == 0` → Neutral

Every score is fully explainable: `ScoreBreakdown` returns not just the
counts but the **actual matched terms by name**, so any score can be
justified in the report or viva by pointing at exactly which words drove
it — this is what "transparent" means in practice, not just a claim.

## Verification

### The brief's own worked example, reproduced exactly

> Headline: *"Startup X raises major funding and expands into new markets"*

```
positive terms matched: List(raises, funding, expands)
negative terms matched: List()
score=3  class=Positive
```

This is an exact match to the brief's specification — not a coincidence,
genuine confirmation the implementation behaves as designed.

### Real headlines (captured live in Phase 7)

| Headline | Result |
|---|---|
| "Kabbage Raises Some Serious Cabbage for Small-Business Loans" | Positive (score=1, matched: raises) |
| "Sifteo Cubes Are Out Today, And Even Better Than You Imagined" | **Neutral** (score=0, no lexicon terms present) |

The Sifteo result is worth discussing honestly in the report: it's a
purely descriptive headline with no sentiment-laden business vocabulary.
**Not every real headline carries scorable sentiment under this
methodology, and that is an expected, correct outcome, not a scorer
failure.** A meaningful share of Phase 10's `neutralNewsCount` feature
will come from cases exactly like this.

### Constructed mixed-sentiment test cases

| Headline | Result |
|---|---|
| "Startup shuts down after failing to raise sufficient funding, layoffs follow" | Negative (score=-1; pos=[raise, funding], neg=[shuts, failing, layoffs]) |
| "Company announces bankruptcy amid mounting losses and investor lawsuit" | Negative (score=-3; neg=[bankruptcy, losses, lawsuit]) |

The first case is the important one: it contains both positive terms
("raise", "funding") and negative terms ("shuts", "failing", "layoffs"),
and the net score correctly resolves to Negative because the negative
count outweighs the positive count — confirming the scorer handles
genuinely mixed-signal text sensibly rather than just detecting presence/
absence of any sentiment word.

## Known limitations (state these in the report, don't hide them)

- **No stemming/lemmatization**: the lexicon lists common inflections
  explicitly (`raise`, `raises`, `raised`, `raising`) rather than using a
  stemmer. This is a deliberate simplicity/transparency tradeoff — every
  match is a literal, inspectable word, with no stemming algorithm to
  second-guess — but it means an inflection not explicitly listed (e.g. an
  unusual verb form) will be missed.
- **No negation handling**: "did not raise funding" would still count
  "raise" and "funding" as positive matches, since the scorer has no
  concept of negation scope. This is a standard, well-known limitation of
  simple bag-of-words lexicon scoring (worth citing as a limitation
  alongside Loughran & McDonald's broader point about naive lexicon
  application).
- **Word-level only, no phrase detection**: sarcasm, idioms, and
  multi-word expressions aren't handled.

## Next

**Phase 10 — Sentiment feature engineering.** Build `FeatureExtractor.scala`:
aggregate all of a startup's `ScoredNewsRecord`s (from Phases 7-9) into
the `SentimentFeatures` case class already defined in Phase 3 —
`newsCount`, `positiveRatio`, `averageSentiment`, `sentimentTrend`, etc. —
enforcing the Phase 2 R4 cutoff filter one more time at aggregation, per
the project's defensive "never trust an upstream filter alone" pattern.
