# Evaluating the Predictive Value of Public News Sentiment for Startup Survival

**Using Scala and Apache Spark**

B.Tech CSE — End-Semester Big Data Project

---

## Research question

> Does incorporating public news sentiment about a startup improve the
> predictive accuracy of a startup survival/closure classifier, compared to
> using structured funding/financial attributes alone?

The methodology is designed so that "yes", "no", and "it depends on the model"
are all valid outcomes. No result is assumed in advance.

## Experimental design (2×2)

|                        | Logistic Regression | Binary SVM   |
|------------------------|---------------------|--------------|
| Financial only         | Experiment 1        | Experiment 2 |
| Financial + Sentiment  | Experiment 3        | Experiment 4 |

Reported per experiment: Accuracy, Precision, Recall, F1, and a confusion
matrix (TP/TN/FP/FN). **All results come from actual execution — nothing in
this repository may contain fabricated numbers.**

## Technology stack

| Layer | Technology | Purpose |
|---|---|---|
| Storage | HDFS | Distributed raw/processed/output storage |
| Warehouse | Apache Hive | Structured startup table, SQL exploration |
| Processing | Scala | Cleaning, tokenisation, name matching, sentiment scoring |
| Distributed compute | Apache Spark (RDD API) | Key/value RDDs, joins, aggregation, persistence |
| ML | Spark MLlib | Logistic Regression, Binary SVM |
| Support | Python (pandas) | One-off EDA and plotting only |

Versions are pinned in Phase 5. Python is confined to exploration and
visualisation; the graded pipeline is Scala + Spark.

## Repository layout

```
startup-survival-prediction/
├── README.md
├── requirements.txt            Python deps (EDA/plotting only)
├── .gitignore
│
├── docs/                       Phase-by-phase design records
│   ├── PHASE00_scope.md        Scope, risks, leakage analysis, team split
│   ├── PHASE01_dataset_analysis.md  Dataset choice + temporal-mismatch finding
│   └── PHASE_PROGRESS.md       Progress tracker
│
├── data/
│   ├── raw/                    Datasets (gitignored; see data/raw/README.md)
│   ├── processed/              Cleaned intermediate output
│   └── sample/                 Small committed samples for testing
│
├── scripts/
│   └── inspect_dataset.py      Phase 1 dataset inspection
│
├── hadoop/hdfs_commands.md     Phase 4
├── hive/                       Phase 4  (schema.sql, analysis_queries.sql)
│
├── scala/src/main/scala/com/startupsurvival/
│   ├── DataCleaner.scala       Phase 6
│   ├── NewsCollector.scala     Phase 7
│   ├── NameMatcher.scala       Phase 8
│   ├── SentimentScorer.scala   Phase 9
│   └── FeatureExtractor.scala  Phase 10
│
├── spark/                      Phases 12–13 (RDD pipeline, join, persistence)
├── ml/                         Phases 16–18 (LR, SVM, evaluation)
├── visualization/              Phase 19
├── dashboard/                  Phase 20
├── results/                    Generated outputs — never hand-edited
└── report/                     Phase 22
```

Files under `scala/`, `spark/`, `ml/`, `hive/`, `hadoop/` and `report/` are
currently **placeholders** naming their owning phase. They are filled in when
that phase is reached.

## Getting started (Phase 1)

```bash
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Download the dataset per `data/raw/README.md`, then:

```bash
python scripts/inspect_dataset.py data/raw/big_startup_secsees_dataset.csv \
    | tee results/phase1_inspection.txt
```

## Important design constraint

The chosen dataset is a Crunchbase snapshot from around **October 2013**,
while collectable news is current. Naively pairing the two would introduce
**twelve years of future information** into the features — catastrophic data
leakage that would invalidate every result.

The project therefore adopts a **retrospective cutoff design**: prediction
cutoff `T = 2013-01-01`, features drawn only from before `T`, label taken from
the post-`T` snapshot. Full reasoning, the verified news-API limitations, and
the fallback plan are in `docs/PHASE01_dataset_analysis.md` §1.4–1.5.

**Read that section before writing any pipeline code.**

## Team

| Member | Area |
|---|---|
| 1 | Data engineering — dataset, HDFS, Hive, ingestion |
| 2 | Scala functional processing — cleaning, matching, sentiment, features |
| 3 | Spark + MLlib — RDDs, joins, feature vectors, LR, SVM |
| 4 | Evaluation, visualisation, dashboard, report, presentation |

## Working rule

Phases are completed and verified in order. Do not begin a phase until the
previous one demonstrably works. Track state in `docs/PHASE_PROGRESS.md`.

## Data attribution

Primary dataset: *Startup Success/Fail Dataset from Crunchbase* (Kaggle, user
`yanmaksi`), published under the Community Data License Agreement – Sharing
v1.0. Raw data is not redistributed in this repository.
