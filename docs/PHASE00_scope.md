# Phase 0 — Project Scope & Feasibility

**Status:** Complete

---

## 1. Project restatement

A binary classification system predicting whether a startup has survived
(still operating) or closed, using two competing feature sets:

- a baseline built only from **structured startup/funding attributes**
- an augmented set that adds **sentiment signals derived from public news
  headlines** about that startup

The deliverable is not simply a model. It is a **controlled comparison
experiment** testing whether unstructured external signal adds measurable
predictive value over structured financial data alone, implemented as a
distributed pipeline (HDFS → Hive → Scala → Spark RDDs → Spark MLlib).

## 2. Research question

> Does incorporating public news sentiment about a startup improve the
> predictive accuracy of a startup survival/closure classifier, compared to
> using structured funding/financial attributes alone?

The methodology must be able to return "yes", "no", or "it depends on the
model" as equally valid outcomes.

## 3. Dependent variable (label)

Startup status, binarised:

- `0` = Closed
- `1` = Operating

Ambiguous statuses (Acquired, IPO, Unknown) are **not** silently collapsed.
See `PHASE01_dataset_analysis.md` §1.6 and Phase 2 for the mapping rules.

## 4. Independent variable groups

**Group A — Structured / financial**
startup age, total funding, funding round count, average round size, largest
round, time since last funding, investor count, industry, country.

**Group B — Sentiment-derived (Scala-computed)**
news_count, positive/negative/neutral counts, positive_ratio, negative_ratio,
average_sentiment, recent_sentiment, sentiment_trend, sentiment_volatility,
news_volume_trend.

Group B is only ever added **on top of** Group A, never used alone — the
question concerns *incremental* value.

## 5. Experimental design (2×2 factorial)

|                        | Logistic Regression | Binary SVM |
|------------------------|---------------------|------------|
| Financial only         | Experiment 1        | Experiment 2 |
| Financial + Sentiment  | Experiment 3        | Experiment 4 |

Two effects can be isolated:

- **Feature effect:** (E3 − E1) and (E4 − E2) — does sentiment help with the
  model held constant?
- **Model effect:** (E2 − E1) and (E4 − E3) — does model choice matter with
  features held constant?

## 6. Syllabus mapping

| Unit | Where it appears |
|------|------------------|
| **Unit 1** — Hadoop, HDFS, MapReduce concepts, batch processing, Hive, Scala basics (type inference, static typing, value types, closures) | HDFS stores raw/processed/output data (Phase 4). Hive holds the structured startup table with SQL-style analytics (Phase 4). Scala type inference, static typing and closures appear throughout preprocessing. |
| **Unit 2** — immutability, immutable collections, Cons/List, tail recursion, higher-order functions (map/filter/fold/partition/span), pattern matching, classes and objects | Implemented directly in `DataCleaner.scala`, `SentimentScorer.scala`, `FeatureExtractor.scala`. Tokenisation, lexicon matching and scoring are built from these primitives rather than delegated to a library. |
| **Unit 3** — RDDs, lineage, DAG, fault tolerance, partitions, transformations/actions, lazy evaluation, persistence, key/value RDDs, MLlib, SVM, Logistic Regression | The Spark pipeline (Phases 12–13) builds Startup and News RDDs as key/value pairs, joins them, persists the result, and feeds Spark MLlib for both models. |

No technology is included decoratively; each maps to a genuine pipeline stage.

## 7. Technical risks (ranked)

1. **Startup–news matching quality** — ambiguous or very short company names
   cause false matches or force conservative no-match labelling, capping how
   much signal sentiment features can carry.
2. **News data availability** — many smaller startups have little or no
   coverage, producing sparse or zero-valued sentiment features.
3. **Class imbalance** — startup datasets skew heavily toward "operating".
4. **Small effective sample after cleaning** — dropping ambiguous statuses,
   duplicates and unmatched records can shrink the usable set sharply.
5. **Version compatibility** — Hadoop/Hive/Scala/Spark mismatches are the most
   common time sink in student setups. Versions are pinned in Phase 5.
6. **Underpowered statistical claims** — with a modest dataset, differences
   between the four experiments may be noise. To be caveated, not overclaimed.

## 8. Data leakage risks

- **Future news leakage** — an article published after a closure that
  describes the closure must never act as a predictive feature. Most severe
  risk; see `PHASE01_dataset_analysis.md` §1.4.
- **Post-cutoff funding leakage** — funding rounds after the cutoff must be
  excluded from Group A.
- **Label-informed sentiment leakage** — articles discussing the outcome
  itself ("X shuts down", "X acquired") must be excluded, since news often
  reports events concurrently with or ahead of formal status updates.
- **Duplicate startup records** — the same entity in both train and test sets.
- **Construct-validity caveat** — companies with more press tend to be larger
  and better funded, so news volume may act as a funding proxy rather than an
  independent signal. Not a leakage bug, but must be discussed in Results.

## 9. Architecture

```
                     +------------------------+
                     |      DATA SOURCES      |
                     |  Startup/Funding CSV   |
                     |  Public News headlines |
                     +-----------+------------+
                                 |
                                 v
                     +------------------------+
                     |          HDFS          |
                     |  /raw /processed /out  |
                     +-------+--------+-------+
                             |        |
                 +-----------+        +-----------+
                 v                                v
        +------------------+           +---------------------+
        |       Hive       |           |  Scala Processing   |
        | structured table |           | clean + tokenise +  |
        | + SQL EDA        |           | match + sentiment   |
        +--------+---------+           +----------+----------+
                 |                                |
                 +---------------+----------------+
                                 v
                      +---------------------+
                      |    Apache Spark     |
                      | Startup RDD (k,v)   |
                      | News RDD (k,v)      |
                      | join / reduceByKey  |
                      | persist()           |
                      +----------+----------+
                                 v
                    +--------------------------+
                    |   Feature Engineering    |
                    | Pipeline A: Financial    |
                    | Pipeline B: Fin+Sentiment|
                    +----------+---------------+
                                 v
                    +--------------------------+
                    |       Spark MLlib        |
                    |  Logistic Regression     |
                    |  Binary SVM              |
                    |  (4 experiments)         |
                    +----------+---------------+
                                 v
                    +--------------------------+
                    |        Evaluation        |
                    | Acc / Prec / Recall / F1 |
                    | Confusion matrix         |
                    +----------+---------------+
                                 v
                    +--------------------------+
                    | Findings + Viz + Demo UI |
                    +--------------------------+
```

## 10. Team division

| Member | Area | Deliverable |
|--------|------|-------------|
| **1** | Data engineering | Dataset sourcing, cleaning, HDFS layout, Hive table + queries → clean startup table queryable by everyone |
| **2** | Scala functional processing | News cleaning, name matching, sentiment scorer, sentiment features → per-startup sentiment table with fixed schema |
| **3** | Spark + MLlib | RDD pipeline, joins, Pipeline A/B feature vectors, LR + SVM → 4 trained models and prediction outputs |
| **4** | Evaluation / viz / docs | Metrics, confusion matrices, visualisations, demo interface, report, slides |

Dependency chain: M1 → M2 → M3 → M4. M1 and M2 parallelise once schemas are
frozen; M4 can scaffold visualisation and report structure early using sample
data.

## 11. Pre-coding checklist

- [x] Dataset candidates evaluated (Phase 1)
- [ ] Dataset downloaded and inspected — **run `scripts/inspect_dataset.py`**
- [ ] Status → {0,1} mapping fixed against real counts
- [ ] Prediction cutoff methodology agreed
- [ ] News source decision confirmed with verified current limitations
- [ ] Hadoop/Hive/Scala/Spark versions pinned for the target environment
- [ ] Hive schema drafted against real columns
- [ ] Group A and Group B feature lists locked
