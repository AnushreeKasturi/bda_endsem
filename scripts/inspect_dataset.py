#!/usr/bin/env python3
"""
PHASE 1 - Dataset Inspection
Project: Evaluating the Predictive Value of Public News Sentiment for
         Startup Survival Using Scala and Apache Spark

Purpose
-------
Characterise the raw Crunchbase-derived dataset BEFORE any pipeline design
decisions are made. This script makes NO assumptions about column names --
it detects what is actually present in the file.

This is throwaway EDA tooling. It is deliberately Python/pandas because the
task is one-off exploration. Every graded pipeline component (cleaning,
sentiment scoring, RDD processing, MLlib) is implemented in Scala + Spark.

Usage
-----
    python scripts/inspect_dataset.py data/raw/big_startup_secsees_dataset.csv

Recommended (saves a copy of the output for the report):
    python scripts/inspect_dataset.py data/raw/big_startup_secsees_dataset.csv \
        | tee results/phase1_inspection.txt
"""

import sys
import pandas as pd
import numpy as np

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 100)

LINE = "=" * 78


def header(title):
    print(f"\n{LINE}\n{title}\n{LINE}")


def find_col(df, candidates):
    """Return the first column present in df matching any candidate name."""
    lowered = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand in lowered:
            return lowered[cand]
    return None


def main(path):
    header("1. LOADING")
    df = pd.read_csv(path, low_memory=False)
    print(f"File           : {path}")
    print(f"Rows           : {len(df):,}")
    print(f"Columns        : {len(df.columns)}")
    print(f"Memory (approx): {df.memory_usage(deep=True).sum() / 1e6:.1f} MB")

    header("2. COLUMN INVENTORY")
    inventory = pd.DataFrame({
        "dtype": df.dtypes.astype(str),
        "non_null": df.notna().sum(),
        "null_count": df.isna().sum(),
        "null_pct": (df.isna().sum() / len(df) * 100).round(2),
        "n_unique": df.nunique(dropna=True),
    })
    print(inventory.to_string())

    header("3. SAMPLE ROWS")
    print(df.head(5).to_string())

    # ---- detect key columns -------------------------------------------
    col_name    = find_col(df, ["name", "company_name", "startup_name"])
    col_perma   = find_col(df, ["permalink", "id", "company_id"])
    col_status  = find_col(df, ["status", "labels", "label", "state"])
    col_funding = find_col(df, ["funding_total_usd", "total_funding",
                                "funding_total", "funding_amount"])
    col_rounds  = find_col(df, ["funding_rounds", "rounds", "num_rounds"])
    col_founded = find_col(df, ["founded_at", "founded", "founded_year"])
    col_first   = find_col(df, ["first_funding_at", "first_funding"])
    col_last    = find_col(df, ["last_funding_at", "last_funding"])
    col_cat     = find_col(df, ["category_list", "category_code",
                                "market", "industry", "category"])
    col_country = find_col(df, ["country_code", "country"])

    header("4. DETECTED KEY COLUMNS")
    for label, col in [
        ("company name", col_name), ("unique id", col_perma),
        ("status/label", col_status), ("total funding", col_funding),
        ("funding rounds", col_rounds), ("founded date", col_founded),
        ("first funding", col_first), ("last funding", col_last),
        ("category", col_cat), ("country", col_country),
    ]:
        mark = "OK  " if col else "MISS"
        print(f"  [{mark}] {label:<16} -> {col}")

    # ---- 5. STATUS DISTRIBUTION (the label) ---------------------------
    header("5. STATUS DISTRIBUTION  (drives label design)")
    if col_status:
        vc = df[col_status].value_counts(dropna=False)
        pct = (vc / len(df) * 100).round(2)
        print(pd.DataFrame({"count": vc, "pct": pct}).to_string())

        closed = df[col_status].astype(str).str.lower().eq("closed").sum()
        operating = df[col_status].astype(str).str.lower().eq("operating").sum()
        if closed and operating:
            total = closed + operating
            print("\n  After dropping acquired/ipo/null:")
            print(f"    closed    (label 0): {closed:,}  ({closed/total*100:.2f}%)")
            print(f"    operating (label 1): {operating:,}  ({operating/total*100:.2f}%)")
            print(f"    usable rows        : {total:,}")
            print(f"    imbalance ratio    : 1 : {operating/closed:.1f}")
    else:
        print("  !! No status column detected - report this back.")

    # ---- 6. FUNDING ---------------------------------------------------
    header("6. FUNDING COLUMN")
    if col_funding:
        raw = df[col_funding]
        print(f"  Raw dtype: {raw.dtype}")
        print(f"  Sample raw values: {raw.dropna().head(5).tolist()}")
        num = pd.to_numeric(
            raw.astype(str).str.replace(r"[,$\s]", "", regex=True)
               .replace({"-": np.nan, "nan": np.nan, "": np.nan}),
            errors="coerce")
        print("\n  Parsed as numeric:")
        print(f"    parse failures/nulls : {num.isna().sum():,} "
              f"({num.isna().sum()/len(df)*100:.1f}%)")
        print(f"    zero-funding rows    : {(num == 0).sum():,}")
        print(f"    min / median / max   : {num.min():,.0f} / "
              f"{num.median():,.0f} / {num.max():,.0f}")
        print("\n  Percentiles:")
        for q in [0.10, 0.25, 0.50, 0.75, 0.90, 0.99]:
            print(f"    p{int(q*100):<3}: {num.quantile(q):>18,.0f}")
    else:
        print("  !! No funding column detected.")

    if col_rounds:
        print("\n  Funding rounds distribution:")
        print(df[col_rounds].value_counts().sort_index().head(15).to_string())

    # ---- 7. DATES (temporal feasibility) ------------------------------
    header("7. DATE COLUMNS  (temporal experiment feasibility)")
    for col in [c for c in [col_founded, col_first, col_last] if c]:
        parsed = pd.to_datetime(df[col], errors="coerce")
        ok = parsed.notna().sum()
        print(f"\n  {col}")
        print(f"    parseable    : {ok:,} / {len(df):,} ({ok/len(df)*100:.1f}%)")
        if ok:
            print(f"    range        : {parsed.min().date()} .. {parsed.max().date()}")
            print(f"    before 2013  : {(parsed < '2013-01-01').sum():,}")
            print("    year counts (top 10):")
            print(parsed.dt.year.value_counts().sort_index().tail(10).to_string())

    # ---- 8. TEMPORAL VIABILITY CHECK ----------------------------------
    header("8. TEMPORAL CUTOFF VIABILITY  (cutoff T = 2013-01-01)")
    if col_status and col_founded and col_first:
        founded = pd.to_datetime(df[col_founded], errors="coerce")
        first_f = pd.to_datetime(df[col_first], errors="coerce")
        st = df[col_status].astype(str).str.lower()
        eligible = (
            st.isin(["closed", "operating"])
            & founded.notna() & (founded < "2013-01-01")
            & first_f.notna() & (first_f < "2013-01-01")
        )
        sub = df[eligible]
        print(f"  Rows surviving all temporal filters: {len(sub):,}")
        if len(sub):
            print("\n  Label balance within eligible subset:")
            evc = sub[col_status].value_counts()
            print(evc.to_string())
            print("\n  => This is your realistic modelling population.")
    else:
        print("  Cannot evaluate - missing status or date columns.")

    # ---- 9. DUPLICATES ------------------------------------------------
    header("9. DUPLICATES")
    print(f"  Fully identical rows: {df.duplicated().sum():,}")
    if col_perma:
        print(f"  Duplicate '{col_perma}': {df[col_perma].duplicated().sum():,}")
    if col_name:
        dupnames = df[col_name].astype(str).str.lower().str.strip()
        print(f"  Duplicate names (case-insensitive): {dupnames.duplicated().sum():,}")
        print("\n  Most repeated names:")
        print(dupnames.value_counts().head(10).to_string())

    # ---- 10. NAME AMBIGUITY (news-matching risk) ----------------------
    header("10. COMPANY NAME AMBIGUITY  (news-matching risk)")
    if col_name:
        nm = df[col_name].astype(str).str.strip()
        lengths = nm.str.len()
        one_word = nm.str.split().str.len().eq(1)
        very_short = lengths <= 4
        print(f"  Single-word names : {one_word.sum():,} "
              f"({one_word.sum()/len(df)*100:.1f}%)  <- high false-match risk")
        print(f"  Names <= 4 chars  : {very_short.sum():,} "
              f"({very_short.sum()/len(df)*100:.1f}%)  <- very high risk")
        print(f"  Median name length: {lengths.median():.0f} chars")
        print("\n  Examples of risky short names:")
        print("   ", nm[very_short].drop_duplicates().head(20).tolist())

    # ---- 11. CATEGORY & COUNTRY --------------------------------------
    header("11. CATEGORY & COUNTRY (categorical feature planning)")
    if col_cat:
        print(f"  '{col_cat}' distinct values: {df[col_cat].nunique():,}")
        print("  Top 15:")
        print(df[col_cat].value_counts().head(15).to_string())
    if col_country:
        print(f"\n  '{col_country}' distinct values: {df[col_country].nunique():,}")
        print("  Top 10:")
        print(df[col_country].value_counts().head(10).to_string())

    header("INSPECTION COMPLETE")
    print("Send the full output back so we can finalise Phase 2.\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/inspect_dataset.py <path-to-csv>")
        sys.exit(1)
    main(sys.argv[1])
