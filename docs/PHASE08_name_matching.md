# Phase 8 — News Cleaning and Startup-Name Matching

**Status:** Complete. `NameMatcher.scala` compiled and verified against
real headlines captured from Phase 7's live Google News RSS run — not
synthetic data.

---

## Methodology

`computeMatchConfidence(companyName, headline)` scores whether a headline
is genuinely about a given startup, in three tiers:

1. **Ambiguous short name** (single word, ≤4 characters — the exact
   threshold Phase 1's inspection script used, kept consistent rather than
   inventing a new definition): confidence capped at 0.3 even on a literal
   match, per the project brief's instruction to mark these unreliable
   rather than force a match.
2. **Exact phrase match**: the full normalized company name appears as a
   whole-word phrase in the headline — confidence 1.0.
3. **Partial multi-word match**: at least half the name's tokens appear
   (not necessarily contiguously) — confidence between 0.5 and 0.8,
   proportional to overlap.
4. **No sufficient overlap**: rejected, confidence 0.0.

Normalization (lowercase, strip punctuation, strip corporate suffixes —
`Inc`, `LLC`, `Ltd`, `Corp`, etc.) is applied identically to both the
company name and the headline before comparison, and uses word-boundary
matching (not substring) so a name doesn't accidentally match as a
fragment inside an unrelated word.

## Verification against real data

### Case 1 — the Kabbage false positive from Phase 7

Phase 7's live run returned an article titled *"The tomato barons of the
occupied Western Sahara"* as a Kabbage result — clearly irrelevant, no
mention of Kabbage anywhere in the title. Run through `NameMatcher`:

```
[REJECT] confidence=0.0  insufficient token overlap (0/1 tokens)
         "The tomato barons of the occupied Western Sahara - Western Sahara Resource Watch"
```

Correctly rejected, as intended.

### An honest correction — a wrong prediction caught by testing

The in-code test originally predicted 4 of 5 real Kabbage headlines would
be accepted. Running it revealed only **2 of 5** were: the two headlines
that literally contain the word "Kabbage" in their title. The other
three — genuinely relevant articles like *"Funding roundup - week ending
08/19/11"* — don't mention the company name in the title at all, even
though the article body almost certainly does.

**This is not a bug in the matcher; it's a real, worth-documenting
limitation of title-only matching.** Google News RSS provides only the
headline text, not the article body, so any relevance signal that lives
only in the body is invisible to this matcher by construction. The
matcher deliberately trades recall (catching every genuinely relevant
article) for precision (never accepting an article that isn't
demonstrably about the company from its title alone) — a defensible,
conservative choice given that a wrongly-accepted article corrupts a
sentiment feature, while a wrongly-rejected one just means slightly less
data. **State this explicitly as a limitation in the final report** — it's
exactly the kind of honest methodological caveat the project brief asks
for, not something to hide.

### Case 2 — Sifteo (real data)

4 of 5 real Sifteo headlines accepted; the 1 rejection follows the same
pattern (title doesn't mention "Sifteo" despite being a relevant article).
Consistent with the Kabbage finding — confirms this is a systematic,
understood behavior, not a one-off anomaly.

### Case 3 — ambiguity threshold

| Name | `isAmbiguousName` | Reasoning |
|---|---|---|
| `Zip` | `true` | Single word, 3 chars |
| `360T` | `true` | Single word, 4 chars |
| `Zendesk` | `false` | Single word, but 7 chars |
| `Instructure` | `false` | Single word, but 11 chars |

`Zip` matched against a plausible headline still returns
`confidence=0.3` (capped), not the usual 1.0 — confirming the ambiguity
guard fires correctly even on a genuine textual match.

### Case 4 — suffix normalization

`normalize("Kabbage Inc.")` and `normalize("Kabbage")` both produce
`"kabbage"` — confirmed equal, so company records with or without a legal
suffix in the source data match consistently.

## What this means for Phase 10

When Phase 10 computes `SentimentFeatures` per startup, it should:
- Only aggregate headlines with `matched = true` from this matcher
- Consider `matchConfidence` as a potential weighting factor (e.g.
  downweighting 0.3-confidence ambiguous-name matches relative to
  1.0-confidence exact matches) rather than treating all accepted matches
  as equally reliable — this is a design decision to make explicitly in
  Phase 10, not assume silently

## Next

**Phase 9 — Custom Scala sentiment scoring.** Build `SentimentScorer.scala`:
a transparent, lexicon-based scorer (never a black-box API call, per the
project brief) using tokenization, stopword handling, and a
positive/negative term lexicon, built from the same functional-programming
toolkit (map/filter/fold, pattern matching, immutable collections) as
Phases 6 and 8.
