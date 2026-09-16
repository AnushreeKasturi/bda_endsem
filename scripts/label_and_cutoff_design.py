#!/usr/bin/env python3
"""
PHASE 2 - Label Design & Prediction Cutoff
Project: Evaluating the Predictive Value of Public News Sentiment for
         Startup Survival Using Scala and Apache Spark

Purpose
-------
Apply the FIXED rules decided in docs/PHASE02_labels_and_cutoff.md to the raw
dataset and produce the single canonical labeled/eligible dataset that every
later phase (Scala cleaning, Hive load, feature engineering, MLlib) builds on.

This is a one-time reference implementation in Python so the rules can be
verified against real data before being re-implemented in Scala in Phase 6.
The Scala DataCleaner in Phase 6 must reproduce these exact rules - if the
row counts printed here ever disagree with Phase 6's output, that is a bug
to fix, not a discrepancy to average away.

Rules implemented (see docs/PHASE02_labels_and_cutoff.md for full rationale)
-----------------------------------------------------------------------------
R1  Cutoff T = 2013-01-01
R2  status -> label:  closed -> 0, operating -> 1, else excluded
R3  Valid date window = [1950-01-01, 2016-12-31]; dates outside this range
    are treated as missing, not as their literal (corrupted) value
R4  A row is ELIGIBLE only if:
      status in {closed, operating}
      AND founded_at is valid and founded_at < T
      AND first_funding_at is valid and first_funding_at < T
    For eligible rows, last_funding_at is only usable as a feature if it is
    also valid and < T; otherwise it is nulled out (not row-dropped).
R5  Deduplication / join key is `permalink`, never `name`.

Usage
-----
    python scripts/label_and_cutoff_design.py \
        data/raw/big_startup_secsees_dataset.csv \
        data/processed/labeled_eligible_startups.csv
"""

import sys
import pandas as pd
import numpy as np

CUTOFF = pd.Timestamp("2013-01-01")
DATE_MIN = pd.Timestamp("1950-01-01")
DATE_MAX = pd.Timestamp("2016-12-31")

LINE = "=" * 78


def header(title):
    print(f"\n{LINE}\n{title}\n{LINE}")


def find_col(df, candidates):
    lowered = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand in lowered:
            return lowered[cand]
    return None


def clean_date(series: pd.Series) -> pd.Series:
    """R3: parse dates; anything outside [DATE_MIN, DATE_MAX] becomes NaT
    rather than being trusted as-is. Returns (cleaned_series, n_corrupted)."""
    parsed = pd.to_datetime(series, errors="coerce")
    out_of_range = parsed.notna() & ((parsed < DATE_MIN) | (parsed > DATE_MAX))
    n_corrupted = int(out_of_range.sum())
    cleaned = parsed.mask(out_of_range, other=pd.NaT)
    return cleaned, n_corrupted


def main(in_path, out_path):
    header("PHASE 2 - LABEL DESIGN & PREDICTION CUTOFF")
    print(f"Input  : {in_path}")
    print(f"Output : {out_path}")
    print(f"Cutoff : T = {CUTOFF.date()}")
    print(f"Valid date window: [{DATE_MIN.date()}, {DATE_MAX.date()}]")

    df = pd.read_csv(in_path, low_memory=False)
    n_raw = len(df)

    col_name    = find_col(df, ["name", "company_name", "startup_name"])
    col_perma   = find_col(df, ["permalink", "id", "company_id"])
    col_status  = find_col(df, ["status", "labels", "label", "state"])
    col_founded = find_col(df, ["founded_at", "founded", "founded_year"])
    col_first   = find_col(df, ["first_funding_at", "first_funding"])
    col_last    = find_col(df, ["last_funding_at", "last_funding"])

    required = {
        "company name": col_name, "permalink": col_perma,
        "status": col_status, "founded_at": col_founded,
        "first_funding_at": col_first,
    }
    missing = [k for k, v in required.items() if v is None]
    if missing:
        print(f"\nFATAL: required columns not found: {missing}")
        print("Check the raw CSV's actual column names and re-run.")
        sys.exit(1)

    header("STEP 1 - RAW LOAD")
    print(f"Rows loaded: {n_raw:,}")

    # ---- R5: dedup key check -------------------------------------------
    header("STEP 2 - DEDUPLICATION KEY (R5)")
    dup_perma = df[col_perma].duplicated().sum()
    print(f"Duplicate '{col_perma}' values: {dup_perma:,}")
    if dup_perma:
        before = len(df)
        df = df.drop_duplicates(subset=[col_perma], keep="first")
        print(f"Dropped {before - len(df):,} duplicate-permalink rows "
              f"(kept first occurrence).")
    else:
        print("None found - permalink confirmed usable as unique key.")

    # ---- R3: date cleaning ----------------------------------------------
    header("STEP 3 - DATE VALIDITY CLEANING (R3)")
    df["_founded_clean"], n_bad_founded = clean_date(df[col_founded])
    df["_first_funding_clean"], n_bad_first = clean_date(df[col_first])
    if col_last:
        df["_last_funding_clean"], n_bad_last = clean_date(df[col_last])
    else:
        df["_last_funding_clean"] = pd.NaT
        n_bad_last = 0

    print(f"founded_at        : {n_bad_founded:,} corrupted values -> nulled")
    print(f"first_funding_at  : {n_bad_first:,} corrupted values -> nulled")
    print(f"last_funding_at   : {n_bad_last:,} corrupted values -> nulled")

    # ---- R2: status -> label ---------------------------------------------
    header("STEP 4 - STATUS -> LABEL MAPPING (R2)")
    status_lower = df[col_status].astype(str).str.strip().str.lower()
    label_map = {"closed": 0, "operating": 1}
    df["_label"] = status_lower.map(label_map)

    vc = status_lower.value_counts(dropna=False)
    print("Raw status distribution:")
    print(vc.to_string())
    excluded_mask = df["_label"].isna()
    print(f"\nExcluded (acquired / ipo / null / other): {excluded_mask.sum():,}")
    print(f"Retained (closed=0 or operating=1)      : {(~excluded_mask).sum():,}")

    df = df[~excluded_mask].copy()
    df["_label"] = df["_label"].astype(int)

    # ---- R4: temporal eligibility ----------------------------------------
    header("STEP 5 - TEMPORAL ELIGIBILITY FILTER (R4, cutoff T)")
    n_before_temporal = len(df)

    founded_ok = df["_founded_clean"].notna() & (df["_founded_clean"] < CUTOFF)
    first_ok = df["_first_funding_clean"].notna() & (df["_first_funding_clean"] < CUTOFF)
    eligible_mask = founded_ok & first_ok

    print(f"Rows with valid status (0/1)              : {n_before_temporal:,}")
    print(f"  ... with founded_at < T                 : {founded_ok.sum():,}")
    print(f"  ... with first_funding_at < T           : {first_ok.sum():,}")
    print(f"  ... satisfying BOTH (eligible)          : {eligible_mask.sum():,}")

    eligible = df[eligible_mask].copy()

    # R4 continued: null out last_funding_at if it's not safely before T
    # (row is kept - only this one feature-relevant field is nulled)
    unsafe_last = eligible["_last_funding_clean"].isna() | (
        eligible["_last_funding_clean"] >= CUTOFF
    )
    n_unsafe_last = int(unsafe_last.sum())
    eligible.loc[unsafe_last, "_last_funding_clean"] = pd.NaT
    print(f"\nlast_funding_at nulled as post-cutoff/unknown-at-T "
          f"(feature-only, row retained): {n_unsafe_last:,} "
          f"({n_unsafe_last/len(eligible)*100:.1f}% of eligible rows)")

    # ---- Final label balance ----------------------------------------------
    header("STEP 6 - FINAL ELIGIBLE POPULATION")
    final_vc = eligible["_label"].value_counts().sort_index()
    n_closed = int(final_vc.get(0, 0))
    n_operating = int(final_vc.get(1, 0))
    print(f"Eligible rows total : {len(eligible):,}")
    print(f"  closed    (0)     : {n_closed:,}  ({n_closed/len(eligible)*100:.2f}%)")
    print(f"  operating (1)     : {n_operating:,}  ({n_operating/len(eligible)*100:.2f}%)")
    if n_closed:
        print(f"  imbalance ratio   : 1 : {n_operating/n_closed:.2f}")

    if len(eligible) < 500:
        print("\n  !! WARNING: eligible population is very small (<500).")
        print("     Statistical comparisons between the 4 experiments may")
        print("     not be meaningful. Stop and reconsider the cutoff.")

    # ---- Build output --------------------------------------------------
    header("STEP 7 - WRITING OUTPUT")
    out_cols = {
        col_perma: "permalink",
        col_name: "name",
    }
    keep_extra = [c for c in df.columns if c not in
                  {col_perma, col_name, col_status, col_founded,
                   col_first, col_last, "_founded_clean",
                   "_first_funding_clean", "_last_funding_clean", "_label"}]

    result = eligible[[col_perma, col_name] + keep_extra].copy()
    result = result.rename(columns=out_cols)
    result["founded_at"] = eligible["_founded_clean"].dt.date
    result["first_funding_at"] = eligible["_first_funding_clean"].dt.date
    result["last_funding_at"] = eligible["_last_funding_clean"].dt.date
    result["label"] = eligible["_label"].values
    result["prediction_cutoff"] = CUTOFF.date()

    result.to_csv(out_path, index=False)
    print(f"Wrote {len(result):,} rows x {len(result.columns)} columns to:")
    print(f"  {out_path}")

    header("PHASE 2 COMPLETE")
    print("Summary for docs/PHASE02_labels_and_cutoff.md / report:")
    print(f"  Raw rows                    : {n_raw:,}")
    print(f"  After dedup on permalink    : {n_before_temporal + excluded_mask.sum():,}")
    print(f"  After status filter (0/1)   : {n_before_temporal:,}")
    print(f"  After temporal cutoff (R4)  : {len(eligible):,}")
    print(f"  Final label balance         : {n_closed:,} closed / {n_operating:,} operating")
    print()


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python scripts/label_and_cutoff_design.py <in_csv> <out_csv>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
