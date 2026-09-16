# Phase 1 — Dataset Selection & Inspection

**Status:** Analysis complete. Awaiting inspection output before Phase 2.

---

## 1.1 Candidate comparison

| Dataset | Scale | Company names? | Status labels | Dates | Verdict |
|---|---|---|---|---|---|
| **yanmaksi** — big-startup-secsees-fail | 66,368 rows, ~11 MB | Yes (`name`, `permalink`) | operating / closed / acquired / ipo | founded_at, first_funding_at, last_funding_at | **Primary choice** |
| **amirataha** — Startups (Crunchbase) | 196,553 rows, 135 MB | Yes | IPO 1.9%, Closed 3.1%, Acquired 9.4%, Operating 85.6% | Yes (44 cols) | Feature-rich backup |
| **chhinna** — Crunchbase company Data | ~2.65 MB zip, multi-file | Yes | Yes | Yes | **Enrichment** (investor counts) |
| **manishkc06** — Startup Success Prediction | ~923 rows | Yes | acquired vs closed only | founded_at, closed_at | Rejected — too small, wrong label space |
| **ajitpasayat** — Startup Success Prediction | Unverified | Unknown | Unknown | Unknown | Skip unless independently inspected |

### Selection

- **Primary:** `yanmaksi/big-startup-secsees-fail-dataset-from-crunchbase`
- **Enrichment (Phase 11, optional):** `chhinna/crunchbase-data` →
  `investments.csv` for `investor_count` / `unique_investor_count`

**Why not amirataha?** 66k clean rows with real company names is the right
size. 196k rows × 135 MB is unnecessarily heavy for single-node Hadoop, and
44 columns are not needed. Real company names are non-negotiable — without
them the news-matching pipeline is impossible.

**Why not manishkc06?** 923 rows leaves roughly 180 test samples; differences
between the four experiments would be statistically meaningless. It also lacks
the "operating" class entirely, which changes the research question.

---

## 1.4 Critical finding — temporal mismatch

**All of these datasets are Crunchbase snapshots from around October 2013.**
The `status` column reflects each company's state as of 2013/2014.

If news is collected today:

```
Company status label:   as of ~2014
Collectable news:       2026
                        ^ twelve years of FUTURE information
```

This is **reverse leakage**. A 2026 article noting that a company shut down
years ago would trivially predict a 2014 label. The model would score near
99% and the result would be worthless. An evaluator who spots this in viva
will dismantle the project.

### The obvious fix does not work

Verified news-source limitations:

- **NewsAPI free (Developer) tier** — 100 requests/day, roughly **1 month**
  of article history, ~24h delay, development use only. A one-month archive
  cannot reach 2013. **Disqualified for historical news.**
- **GNews free tier** — historical archive only reaches back to 2020. Also
  disqualified.
- **Google News RSS** — free and effectively unmetered, but archival depth
  for obscure 2013 startups is unverified and likely poor. Whether the RSS
  endpoint honours `after:` / `before:` date operators **must be tested
  empirically** — no claim either way until measured.

There is no free route to 2012–2013 headlines for 66,000 obscure startups.

---

## 1.5 Resolution paths

### Path A — ignore it
Collect 2026 news, predict 2014 labels. Guaranteed leakage. **Rejected.**

### Path B — historical subset with archival news
Cutoff `T = 2013-01-01`. Restrict to the ~300–1,500 best-funded, most
newsworthy startups. Retrieve pre-2013 headlines via Google News RSS with
date operators.

- *Pro:* scientifically ideal if coverage exists.
- *Con:* high risk of no coverage; potentially two wasted weeks.
- *Action:* 30-minute feasibility spike in Phase 7 before committing.

### Path C — reframed retrospective design (**recommended**)

> **Prediction cutoff T = 2013-01-01.**
> **Features:** funding/founding information dated strictly before `T`, plus
> news sentiment from the window `[T − 24 months, T)`.
> **Label:** status as recorded in the 2013/2014 snapshot — the outcome
> observed *after* `T`.

A genuine, leakage-free retrospective design, matching the methodology used in
published startup-survival work. The open question is only whether the news
window can be populated, which the Phase 7 spike answers.

**Fallback:** if archival coverage fails, use a clearly-labelled, reproducible
**synthetic news corpus** generated under documented assumptions, to validate
the *pipeline* rather than to make real-world claims. Done transparently this
is academically acceptable — it shifts the contribution from "sentiment
predicts survival" to "a distributed pipeline for evaluating whether sentiment
predicts survival, validated end to end", which still satisfies a Big Data
course graded on Hadoop/Hive/Scala/Spark/MLlib competence.

**Decision: adopt Path C, run the Path B spike in Phase 7, keep the synthetic
fallback in reserve.** All later phases are designed so the fallback is a
one-file swap rather than a redesign.

---

## 1.6 Preliminary label mapping

To be confirmed against real counts in Phase 2.

| Status | Proposed | Reasoning |
|---|---|---|
| `closed` | **0** | Unambiguous failure |
| `operating` | **1** | Unambiguous survival |
| `acquired` | **Exclude** | Ambiguous — acqui-hires are disguised failures, strategic exits are successes; the dataset cannot distinguish them. Including them injects label noise. |
| `ipo` | **Exclude** | ~1.9% of records; clearly survived, but a structurally different (older, larger) population that inflates the majority class with easy cases. |
| null / blank | **Exclude** | No label |

**Expected consequence:** with roughly 85.6% operating and 3.1% closed,
dropping acquired and IPO leaves an imbalance near **96:4**. Severe. To be
reported openly and handled in Phases 14–15 — never hidden, never silently
resampled.

---

## 1.7 Inspection procedure

Script: `scripts/inspect_dataset.py`

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

python scripts/inspect_dataset.py data/raw/big_startup_secsees_dataset.csv \
    | tee results/phase1_inspection.txt
```

The script detects columns rather than assuming names, and reports: column
inventory with null rates, status distribution and resulting imbalance,
funding parsing behaviour, date coverage, temporal-cutoff viability, duplicate
analysis, and company-name ambiguity (news-matching risk).

### Expected output

These are predictions from published descriptions of the dataset, **not actual
execution results**. Verify against the real run; if they disagree, the real
output is authoritative.

- ~66,000 rows, ~10–14 columns
- Status values: `operating`, `closed`, `acquired`, `ipo`
- Roughly 85% operating, ~3% closed
- `funding_total_usd` stored as **text**, using `-` for missing values
- `founded_at` with substantial nulls, likely 20–40%
- Maximum dates around 2013–2014, confirming the snapshot era
- A meaningful count of single-word / very short names flagged as risks

### Checklist

- [ ] Script runs without error
- [ ] Row count and exact column names recorded
- [ ] Exact status values and counts recorded
- [ ] Section 8 reports a usable population — **flag immediately if under ~2,000**
- [ ] Maximum date confirms the ~2013/2014 snapshot
- [ ] Section 1.4 understood and Path C confirmed

---

## Next

**Phase 2 — Label design and prediction cutoff.** Fix the status → {0,1}
mapping against real counts, formalise the cutoff-`T` methodology, define
which fields are feature-eligible versus label-only, and write the
leakage-prevention rules binding all later phases.
