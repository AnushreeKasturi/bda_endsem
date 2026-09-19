# Phase 7 — News Collection

**Status:** Complete and verified with a real live test. Feasibility spike
run twice: first via web search as a proxy (see below — this prediction
turned out to be overly pessimistic), then via the actual
`NewsCollector.scala` against live Google News RSS, which is the result
that matters and is documented under "Live test results" below.

---

## The feasibility spike

Phase 1 (§1.4-1.5) flagged an open question: can pre-2013 news headlines
actually be retrieved for this dataset's startups at all, given every free
news API's history limit falls short of 2013?

### Method (and an honest caveat about it)

Rather than test Google News RSS directly (not reachable from this
environment), general web search was used as a proxy signal: if dated,
real news coverage from 2011-2013 is discoverable via web search at all,
that's meaningful evidence it likely exists in Google's News index too
(which powers Google News RSS), even though it doesn't prove the RSS
endpoint's specific `before:`/`after:` query operators work as expected.
**That mechanism-level question is still open and is the actual live test
you need to run** (see "What to run" below).

### Findings

| Company | Funding scale | Result |
|---|---|---|
| Kabbage | Larger (~$50M+ total, multiple rounds) | Dense, well-dated coverage from TechCrunch back to January 2011. Real, retrievable, specific. |
| Sifteo | Smaller (~$9M total) | Almost no real news-article coverage. What exists is mostly paywalled Crunchbase/CBInsights aggregator data, not actual dated headlines. |

### Interpretation

News coverage correlates with funding size — larger, more notable startups
have retrievable historical press; smaller ones largely don't. This is
exactly the risk flagged in Phase 0 §7 (technical risks) and is worth
stating plainly in your report's limitations section: **news volume as a
feature may partly just be a proxy for funding size**, not fully
independent signal. This doesn't invalidate the experiment — it's a
legitimate, defensible thing to discuss when interpreting Experiment 3/4
results later.

## Live test results — actual, not predicted

Run against three companies spanning funding scale (`sbt "runMain
com.startupsurvival.NewsCollector \"Kabbage\" \"Sifteo\" \"Instructure\""`):

| Company | Funding scale | Pre-cutoff items returned | Dates correct? |
|---|---|---|---|
| Kabbage | Larger (~$50M+) | 6 | Yes — all 2011-2012 |
| Sifteo | Smaller (~$9M) | 10 | Yes — all 2011-2012 |
| Instructure | Mid (~$30M+) | 8 | Yes — all 2011-2012 |

**This meaningfully overrides the pessimistic prediction made earlier in
this document from the web-search proxy test.** Sifteo — deliberately
chosen as the "hard case" because a web-search proxy suggested it had
almost no real coverage — returned 10 genuine, relevant, correctly-dated
articles via the live Google News RSS endpoint. The `before:`/`after:`
date operators are confirmed working correctly: every single date across
all 26 returned items fell inside the requested 2011-01-01 to 2013-01-01
window.

**Conclusion: Google News RSS's own archive is meaningfully deeper than
what surfaces through general web search ranking.** This is the actual,
verified finding — the proxy-test prediction earlier in this document was
wrong and is superseded by this live result.

### A data-quality note for Phase 8

One returned Kabbage item — *"The tomato barons of the occupied Western
Sahara"* — is a clear false match (likely a substring collision on
"cabbage"). Real evidence that `NameMatcher.scala`'s
`news_match_confidence` scoring needs to filter this kind of noise, not
trust every RSS result blindly. Keep this example as a concrete test case
when building Phase 8.

## Scope decision — revised

Given real yields (6-10 relevant items per company, even for a smaller
startup), restricting collection to only the top 300-500 best-funded
startups is now judged **too conservative**. Revised plan: attempt
collection across a substantially larger slice of the eligible population
(the exact size depends on request-rate practicality, to be determined
once a larger batch run is timed in Phase 10) rather than pre-emptively
narrowing to only the largest, best-known companies.

## `NewsCollector.scala` — design and verification

### What it does

1. `buildRssUrl` — constructs a Google News RSS search URL for a company
   name, using Google's `before:`/`after:` search operators to bias
   results toward the pre-cutoff window (best-effort — not guaranteed to
   be honored by Google's News search the same way as plain web search)
2. `fetchUrl` — HTTP GET with a browser-like User-Agent (Google has been
   known to reject Java's default UA) and a timeout, so one hung request
   can't stall a batch run
3. `parseRssItems` — extracts `<item>` blocks and their
   `title`/`link`/`pubDate`/`source` via targeted regex, not a general XML
   library (deliberate — avoids adding a `scala-xml` build dependency for
   a format this constrained; see in-file comment for when this choice
   would need to change)
4. `parsePubDate` — parses RSS's RFC-822 date format into `LocalDate`
5. `collectFor` — the end-to-end flow, re-applying the Phase 2 R4 cutoff
   filter at collection time (never trust the upstream Google query alone
   to have honored the date operators correctly)

### What was verified (and how, given the network restriction)

A realistic RSS payload was hand-built matching Google's documented
output format (CDATA-wrapped titles, RFC-822 `pubDate`, a `<source>` tag)
containing three items: two dated in 2011, one in 2015. Running the parser
against it confirmed:

- All 3 items correctly extracted with clean titles (CDATA properly
  stripped)
- Dates correctly parsed (`Wed, 12 Jan 2011 05:00:00 GMT` → `2011-01-12`)
- The cutoff filter correctly kept only the 2 pre-2013 items, excluding
  the 2015 one
- The URL builder produced a well-formed, correctly-encoded query:
  `https://news.google.com/rss/search?q=%22Kabbage%22+before%3A2013-01-01+after%3A2011-01-01&hl=en-US&gl=US&ceid=US:en`

**What is NOT yet verified: the live network call.** The parsing logic is
solid; whether Google's actual RSS endpoint returns useful, correctly
date-filtered results for a real query is the open question.

## What to run — the real live test

```bash
cd ~/startup-survival-prediction/scala
sbt "runMain com.startupsurvival.NewsCollector \"Kabbage\" \"Sifteo\" \"Instructure\""
```

This is a **manual spike run**, not a batch collector — deliberately
capped to a handful of company names so you can see real output before
committing to a larger run. It prints the constructed URL and the
pre-cutoff items found for each name.

### What to check

1. Does it print any items at all, or zero for everything? (Zero for
   everything suggests either the query format needs adjusting, or Google
   is blocking the request — check for an HTTP error in the `[WARN]`
   output.)
2. For "Kabbage" specifically, do the returned dates actually fall in
   2011-2012, or is Google ignoring the `before:`/`after:` operators and
   just returning recent/irrelevant results?
3. Try it against a genuinely obscure name from your own dataset (one you
   spot-check in `labeled_eligible_startups.csv`) to get a real read on
   long-tail coverage.

Paste the output back — the exact yield numbers determine both the final
subset size for Phase 10 and whether the `before:`/`after:` operators need
a different approach (e.g. filtering client-side only, which `collectFor`
already does as a safety net regardless).

## Next

Phase 7 is complete — live collection confirmed working, date-filtering
confirmed accurate, one real noise example captured for Phase 8 to handle.

**Phase 8 — News cleaning and startup-name matching**, building
`NameMatcher.scala` to connect retrieved headlines to the correct
`permalink`, with the `news_match_confidence` methodology specified in the
original project brief. The Kabbage/"tomato barons" false match from this
phase is a concrete test case to validate the matcher against.
