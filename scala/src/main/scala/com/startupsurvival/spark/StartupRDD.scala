package com.startupsurvival.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import com.startupsurvival.DataCleaner
import com.startupsurvival.FinancialFeatureExtractor
import com.startupsurvival.models.{StartupRecord, FinancialFeatures}

/**
 * PHASE 12 - Spark RDD Pipeline: Startup RDD
 *
 * Loads the real Phase 2 output (22,075 rows, already uploaded to HDFS in
 * Phase 4) into a key/value RDD[(String, StartupRecord)] keyed by
 * permalink, reusing Phase 6's DataCleaner parsing functions directly -
 * the exact same quote-aware CSV parser and Option-based field parsing
 * that was hand-verified against real data in Phase 6, now running
 * distributed instead of on a single JVM.
 *
 * NOT YET RUN OR COMPILED BY ME - my development environment has no
 * Spark installation and no network access to fetch one. This file
 * follows standard, well-established Spark 3.5.x / Scala 2.12 RDD API
 * usage, but you are the first to actually compile and run it. Report
 * back the real output so we can verify it together (same pattern as
 * Phase 7's live NewsCollector test).
 *
 * ============================================================================
 * SYLLABUS UNIT 3 CONCEPTS DEMONSTRATED - READ THIS FOR THE REPORT/VIVA
 * ============================================================================
 * RDD CREATION: sc.textFile() reads the HDFS file into an RDD[String],
 *   one element per line. This is lazy - no data is actually read from
 *   HDFS yet at this point, only a plan (lineage) is recorded.
 *
 * TRANSFORMATIONS (all LAZY - none of these execute anything by
 * themselves; they only build up the RDD's lineage graph):
 *   - filter()   : drops the CSV header line
 *   - map()      : parses each line into a List[String] of fields
 *                  (DataCleaner.parseCsvLine)
 *   - flatMap()  : parses each field-list into an Option[StartupRecord]
 *                  and drops the Nones in one pass (DataCleaner.rowToStartupRecord)
 *   - map()      : keys each StartupRecord by its permalink
 *   - mapValues(): (in extractFinancialFeaturesRDD) transforms only the
 *                  value of each pair, leaving the key untouched - more
 *                  efficient than map() + re-pairing when the key doesn't
 *                  need to change
 *
 * ACTIONS (these DO trigger real computation across the cluster/executors
 *   - this is the only point at which the lazy transformation chain above
 *   actually runs):
 *   - count()  : triggers a full pass over the data to count elements
 *   - take(n)  : triggers just enough computation to produce n elements
 *   - collect(): triggers full computation and pulls all results back to
 *                the driver - used sparingly below, only on small,
 *                already-aggregated results, never on the full 22,075-row
 *                RDD (collecting the whole dataset to the driver defeats
 *                the point of distributed processing and risks driver
 *                memory issues on a larger dataset)
 *
 * LAZY EVALUATION: because transformations are lazy, writing
 *   `val rdd = sc.textFile(...).filter(...).map(...)` does no real work.
 *   Spark only builds a DAG (directed acyclic graph) of how to compute
 *   the RDD, and actually executes it (as a set of stages and tasks
 *   across partitions) only when an action like count() or take() is
 *   called. This is why persist() (below) matters: without it, calling
 *   two separate actions on the same RDD re-runs the ENTIRE lineage twice.
 *
 * PARTITIONS: sc.textFile() splits the HDFS file into partitions
 *   (by default, roughly aligned with HDFS block boundaries). Each
 *   partition is processed independently and in parallel by a task.
 *   Transformations like map/filter/flatMap are "narrow" - each output
 *   partition depends on exactly one input partition, no data movement
 *   between partitions is needed.
 *
 * LINEAGE AND FAULT TOLERANCE: Spark does not replicate RDD data itself;
 *   instead, it remembers the sequence of transformations (the lineage)
 *   that produced each RDD from its original source. If a partition is
 *   lost (e.g. an executor crashes), Spark recomputes just that partition
 *   from the recorded lineage rather than needing a backup copy of the
 *   data - this is the core of Spark's fault tolerance model.
 *
 * PERSISTENCE: startupRDD is used by multiple downstream actions in this
 *   file (count, take, the financial-features action). persist() tells
 *   Spark to keep the computed partitions in memory (or disk, depending
 *   on storage level) after the first action realizes them, so the
 *   second and third actions don't repeat the textFile+parse work from
 *   scratch. Without persist(), each action would independently re-read
 *   and re-parse the full HDFS file due to lazy evaluation.
 * ============================================================================
 */
object StartupRDD {

  val HdfsPath = "hdfs://localhost:9000/startup-survival/processed/startups/labeled_eligible_startups.csv"

  /**
   * Loads and parses the real HDFS dataset into a persisted key/value
   * RDD[(String, StartupRecord)]. Reuses DataCleaner's pure parsing
   * functions (Phase 6) unchanged - referencing a Scala `object`'s pure
   * functions from inside a Spark closure works because the object
   * itself, containing no mutable or non-serializable state, serializes
   * cleanly to each executor.
   */
  def load(spark: SparkSession, path: String = HdfsPath): RDD[(String, StartupRecord)] = {
    val sc = spark.sparkContext

    val rawLines: RDD[String] = sc.textFile(path)

    val header = rawLines.first() // an action - triggers reading just the first line/partition

    val startupRDD: RDD[(String, StartupRecord)] =
      rawLines
        .filter(_ != header)
        .map(DataCleaner.parseCsvLine)
        .flatMap(DataCleaner.rowToStartupRecord)
        .map(record => (record.permalink, record))

    // Persisted because count(), take(), and the financial-features demo
    // below all act on this same RDD - see the class-level comment on why
    // this avoids redundant recomputation of the full parse chain.
    startupRDD.persist()
  }

  /**
   * Demonstrates mapValues (a key-preserving transformation) to derive
   * Phase 11's FinancialFeatures from each StartupRecord, without
   * re-keying the RDD.
   */
  def extractFinancialFeaturesRDD(
      startupRDD: RDD[(String, StartupRecord)]
  ): RDD[(String, FinancialFeatures)] =
    startupRDD.mapValues(FinancialFeatureExtractor.extractFinancialFeatures)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("StartupRDD-Phase12")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("=== Loading Startup RDD from HDFS ===")
    val startupRDD = load(spark)

    // ACTION 1: count() - expect 22075, matching every prior verification
    // (Phase 2 Python, Phase 4 Hive, Phase 5 Spark DataFrame, Phase 6
    // plain-Scala DataCleaner all independently produced this number).
    val total = startupRDD.count()
    println(s"Total startups loaded: $total")
    println("Expect: 22075 (matches every prior phase's verification)\n")

    // ACTION 2: take(3) - a partial action, only materializes 3 elements
    println("=== Sample records (take(3)) ===")
    startupRDD.take(3).foreach { case (permalink, record) =>
      println(s"  $permalink -> label=${record.label}, founded=${record.foundedAt}, funding=${record.fundingTotalUsd}")
    }

    // TRANSFORMATION DEMO: filter + count by label, using key/value RDD
    // operations (map to (label, 1) pairs, then reduceByKey to sum) -
    // this is the idiomatic Spark word-count-style pattern, applied here
    // to count closed vs operating startups distributed across partitions
    // rather than collecting everything to the driver first.
    println("\n=== Label distribution via reduceByKey (distributed count) ===")
    val labelCounts: RDD[(Int, Int)] =
      startupRDD
        .map { case (_, record) => (record.label, 1) }
        .reduceByKey(_ + _) // reduceByKey causes a shuffle - a stage boundary in the DAG,
                             // unlike the narrow map/filter transformations above
    labelCounts.collect().sortBy(_._1).foreach { case (label, count) =>
      println(s"  label=$label -> count=$count")
    }
    println("Expect: label=0 -> 2902, label=1 -> 19173\n")

    // Financial features via mapValues, demonstrating Phase 11's
    // leakage-guarded extractor running inside the distributed pipeline.
    println("=== Financial features sample (mapValues) ===")
    val financialRDD = extractFinancialFeaturesRDD(startupRDD)
    financialRDD.take(3).foreach { case (permalink, features) =>
      println(s"  $permalink -> ageYears=${"%.1f".format(features.startupAgeYears)}, " +
        s"leakageSafe=${features.fundingDataLeakageSafe}, totalFunding=${features.totalFundingUsd}")
    }

    val leakageSafeCount = financialRDD.filter(_._2.fundingDataLeakageSafe).count()
    println(s"\nStartups with leakage-safe funding data: $leakageSafeCount / $total " +
      s"(${"%.1f".format(leakageSafeCount.toDouble / total * 100)}%)")

    spark.stop()
  }
}
