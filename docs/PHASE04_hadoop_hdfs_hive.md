# Phase 4 — Hadoop / HDFS / Hive Setup

**Status:** Complete. Verified against a live single-node cluster (WSL2,
Ubuntu 24.04). All sanity checks in `hive/schema.sql` match Phase 2's
reported numbers exactly.

---

## What was installed

| Component | Version | Notes |
|---|---|---|
| Java | OpenJDK 8 (1.8.0_502) | Chosen for best documented compatibility across Hadoop/Hive/Spark |
| Hadoop | 3.3.6 | HDFS + YARN, single-node pseudo-distributed mode |
| Hive | 3.1.3 | Downloaded from `archive.apache.org` (moved off the main mirror) |

## Setup sequence (for redoing this on a new machine)

1. Java 8 installed via `apt`
2. Passwordless SSH to `localhost` configured (required even for single-node)
3. Hadoop 3.3.6 downloaded, `HADOOP_HOME` + related env vars set in `.bashrc`
4. `core-site.xml`, `hdfs-site.xml` configured; `JAVA_HOME` uncommented in
   `hadoop-env.sh` (was left commented by default — easy miss)
5. NameNode formatted (`hdfs namenode -format`) — **one-time only**
6. HDFS started (`start-dfs.sh`), verified via `jps`
   (NameNode/DataNode/SecondaryNameNode)
7. **`mapred-site.xml` and `yarn-site.xml` configured, YARN started
   (`start-yarn.sh`)** — see "Issue 2" below; this step was initially
   skipped and had to be added
8. `/startup-survival/{raw,processed,output}/...` directory tree created in
   HDFS; Phase 1's raw CSV and Phase 2's labeled CSV uploaded
9. Hive 3.1.3 downloaded and extracted; `HIVE_HOME` set in `.bashrc`
10. Guava version conflict between Hive 3.1.3 and Hadoop 3.3.6 patched
    (Hive ships an old `guava-19.0.jar` that conflicts with Hadoop's newer
    one — replaced with Hadoop's version)
11. `hive-site.xml` written to pin the Derby metastore to a fixed absolute
    path (`$HIVE_HOME/metastore_db`) — see "Issue 1" below
12. `schematool -dbType derby -initSchema` run to initialize the metastore
13. `hive/schema.sql` run; sanity checks verified against Phase 2's numbers

## Issues encountered and fixed

### Issue 1 — metastore not found depending on launch directory

Symptom: `HiveException: Unable to instantiate ... SessionHiveMetaStoreClient`.

Cause: `schematool -initSchema` was first run from `~`, creating an
embedded Derby metastore at `~/metastore_db`. Launching `hive` later from a
different directory (`~/startup-survival-prediction`) caused Derby to look
for the metastore relative to *that* directory instead, where it didn't
exist.

Fix: added `hive-site.xml` with an absolute
`javax.jdo.option.ConnectionURL`, pinning the metastore to one fixed path
regardless of the shell's current directory. Re-ran `schematool -initSchema`
after removing the stray `~/metastore_db`.

### Issue 2 — MapReduce jobs failing instantly with return code 2

Symptom: `FAILED: Execution Error, return code 2 from
org.apache.hadoop.hive.ql.exec.mr.MapRedTask`, `HDFS Read: 0`, job fails
before touching any data. Intermittent — occasionally a query would
succeed, most of the time it failed.

Cause: YARN (`mapred-site.xml`, `yarn-site.xml`) had never been configured
or started. Hive was falling back to an unreliable in-process local job
runner ("Job running in-process (local Hadoop)" in the logs) instead of
submitting through a real resource manager.

Fix: configured `mapreduce.framework.name=yarn` and the NodeManager
auxiliary services, then `start-yarn.sh`. Confirmed via `jps` showing
`ResourceManager` and `NodeManager` alongside the three HDFS daemons.
Queries then ran reliably through YARN (~30-40s per query on this hardware
— cold JVM/container startup, not a performance concern for this project's
scale).

### Issue 3 — CSV column misalignment (the serious one)

Symptom: `SELECT COUNT(*)` correctly returned 22,075, but
`GROUP BY label` returned `NULL: 1031`, `0: 2792` — no `1` group at all,
and the totals didn't add up to 22,075.

Cause: the original table definition used
`ROW FORMAT DELIMITED FIELDS TERMINATED BY ','`, which splits on every
comma with no understanding of quoting. Phase 2's Python script (via
pandas `to_csv()`) correctly double-quotes any field containing a comma
(e.g. a company name or category list with an embedded comma) per
standard CSV convention — but Hive's naive delimiter does not respect
those quotes. Any row with a comma inside a quoted field had every column
after it shifted by one or more positions, corrupting `label` (and
silently corrupting other columns too) for an unknown but non-trivial
subset of rows, while the *line* count itself remained correct.

This was caught specifically because Phase 2's script cross-checked label
balance as a standalone number, and querying the Hive table gave a
different number than the Python script did on the same underlying data —
the Hive layer was the one with the bug.

Fix: switched to `org.apache.hadoop.hive.serde2.OpenCSVSerde`, which
correctly parses quoted, comma-containing fields. OpenCSVSerde requires all
columns to be declared `STRING`, so the design is:

- `startups_eligible_raw` — external table, all columns `STRING`, read via
  OpenCSVSerde
- `startups_eligible` — a `VIEW` on top of the raw table, `CAST`-ing each
  column to its intended type (`INT`, `DATE`, `TINYINT`)

**All later phases query the view `startups_eligible`, never the raw
table.** After the fix, every sanity check matched Phase 2's Python output
exactly (see table below).

## Verified results (matches Phase 2 exactly)

| Check | Phase 2 expected | Hive result | Match |
|---|---|---|---|
| Total rows | 22,075 | 22,075 | Yes |
| Label 0 (closed) | 2,902 | 2,902 | Yes |
| Label 1 (operating) | 19,173 | 19,173 | Yes |
| Null permalinks | 0 | 0 | Yes |
| Duplicate permalinks | 0 rows | 0 rows | Yes |
| Leakage violations (founded_at >= cutoff) | 0 | 0 | Yes |

## Concepts demonstrated (for report / viva)

- **NameNode** — holds filesystem metadata (what files/blocks exist and
  where); does not store file data itself.
- **DataNode** — stores the actual data blocks on local disk. Single node
  here means a single DataNode.
- **SecondaryNameNode** — periodically checkpoints the NameNode's edit log;
  is *not* a hot-standby/failover NameNode, a common misconception worth
  addressing directly in viva.
- **Replication factor** — set to 1 here (single DataNode); production
  clusters use 3 for fault tolerance. The trade-off (storage cost vs.
  resilience to node failure) is the reason for the difference.
- **YARN (ResourceManager/NodeManager)** — schedules and executes
  MapReduce jobs across the cluster; Hive compiles HiveQL into MapReduce
  jobs and submits them through YARN rather than executing SQL directly.
- **SerDe (Serializer/Deserializer)** — Hive's mechanism for reading
  arbitrary file formats into rows/columns. `OpenCSVSerde` here versus the
  naive delimited reader is a concrete, defensible example of *why* SerDe
  choice matters, not just a definition to recite.
- **External table vs. view** — the external table owns no data (HDFS
  remains authoritative, `DROP TABLE` doesn't delete the file); the view
  adds a typed, query-friendly layer without duplicating storage.

## Next

**Phase 5 — Scala + Spark environment setup.** Install Scala and Apache
Spark, set up the sbt project structure for
`scala/src/main/scala/com/startupsurvival/`, and verify the environment can
compile the case classes already drafted in Phase 3.
