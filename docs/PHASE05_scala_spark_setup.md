# Phase 5 — Scala + Spark Environment Setup

**Status:** Complete. Verified: sbt compiles the Phase 3 case classes
cleanly, and `spark-shell` reads the real HDFS data with a matching row
count (22,075).

---

## What was installed

| Component | Version | Notes |
|---|---|---|
| SDKMAN | 5.23.0 | Version manager for Scala/sbt/Spark |
| Scala | 2.12.18 | Managed per-project by sbt; no separate system `scala` binary needed |
| sbt | 1.9.9 | Pinned deliberately — sbt 2.x (SDKMAN's default) requires JDK 17+, incompatible with the JDK 8 kept for Hadoop/Hive |
| Apache Spark | 3.5.1 | Paired with Scala 2.12.18, runs on the existing JDK 8 |

## Setup sequence

1. SDKMAN installed (required `zip` — was missing; `unzip` had been
   installed in Phase 1 but not its counterpart)
2. `sdk install scala 2.12.18` — set as default
3. `sdk install sbt` — SDKMAN's default pulled sbt 2.0.9, which failed with
   `sbt 2.x requires JDK 17 or above, but you have JDK 8`. Explicitly
   installed and defaulted to `sbt 1.9.9` instead, which supports JDK 8.
4. sbt project scaffolded at `scala/`: `build.sbt` (Scala 2.12.18, Spark
   3.5.1 `% "provided"` dependencies) and `project/build.properties`
5. `sbt compile` — succeeded first attempt, compiling `StartupRecord.scala`
   and `NewsRecord.scala` from Phase 3
6. `sdk install spark 3.5.1` — one transient network timeout mid-download
   (curl error 56), resolved by simple retry
7. `spark-shell --version` confirmed: Spark 3.5.1, Scala 2.12.18, OpenJDK
   1.8.0_502 — the intended combination
8. HDFS read-test: `spark.read.csv(...)` against
   `hdfs://localhost:9000/startup-survival/processed/startups/labeled_eligible_startups.csv`
   returned `count() = 22075`, matching every prior verification (Phase 2
   Python output, Phase 4 Hive view)

## Why sbt 1.9.9 specifically, not the SDKMAN default

This is worth stating explicitly in the report: SDKMAN's `sdk install sbt`
with no version pulls the newest release, which was sbt 2.0.9 at the time
of this project. sbt 2.x requires JDK 17+. Upgrading Java system-wide was
rejected to avoid destabilizing the already-verified Hadoop/Hive setup
(Phase 4), which was deliberately pinned to JDK 8 for compatibility
reasons of its own. Pinning sbt to 1.9.9 (which supports JDK 8) resolves
this without touching Java at all — a deliberate, documented compatibility
decision, not an oversight.

## Verified end-to-end consistency

The same dataset has now been independently counted by three different
tools, all agreeing:

| Layer | Tool | Row count |
|---|---|---|
| Phase 2 | Python / pandas | 22,075 |
| Phase 4 | Hive (via `startups_eligible` view) | 22,075 |
| Phase 5 | Spark (`spark.read.csv` from HDFS) | 22,075 |

This is meaningful, not just a repeated check — Hive's naive CSV parsing
was proven buggy in Phase 4 (the OpenCSVSerde fix), and Spark's CSV reader
uses different underlying parsing logic entirely. All three independently
landing on the identical number is real evidence the data pipeline is
sound, not just that the same bug was propagated silently through every
layer.

## Next

**Phase 6 — Startup data preprocessing (Scala).** Implement
`DataCleaner.scala`: parse HDFS/Hive data into `StartupRecord` instances,
handle `funding_total_usd` string-to-Double parsing (the ~19.3% "-"
placeholder case from Phase 1), and demonstrate the Unit 2 syllabus
concepts (immutable collections, higher-order functions, pattern matching)
on real data for the first time in this project.
