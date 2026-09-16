# data/raw/

Raw datasets are **not committed to version control** (see `.gitignore`).
Each team member downloads them locally.

## Required file (Phase 1)

**`big_startup_secsees_dataset.csv`**

Source: https://www.kaggle.com/datasets/yanmaksi/big-startup-secsees-fail-dataset-from-crunchbase

Approx. 66,368 rows, ~11 MB. Derived from a Crunchbase snapshot dated
around October 2013.

### Download

Manual: download the CSV from the Kaggle page above and place it in this
directory.

Via the Kaggle CLI:

    pip install kaggle
    # place your kaggle.json API token at ~/.kaggle/kaggle.json, then:
    kaggle datasets download -d yanmaksi/big-startup-secsees-fail-dataset-from-crunchbase
    unzip big-startup-secsees-fail-dataset-from-crunchbase.zip -d .

## Optional enrichment (Phase 11)

**`chhinna/crunchbase-data`** — multi-file Crunchbase export, used to derive
`investor_count` / `unique_investor_count`, which the primary dataset lacks.

Source: https://www.kaggle.com/datasets/chhinna/crunchbase-data

## Licensing note

The primary dataset is published on Kaggle under the Community Data License
Agreement (Sharing, v1.0). Cite the Kaggle source in the report. Do not
redistribute the raw file in your submission archive.
