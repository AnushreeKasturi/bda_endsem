package com.startupsurvival.spark

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.ml.linalg.{Vector, Vectors, SQLDataTypes}
import org.apache.spark.rdd.RDD
import java.time.LocalDate
import com.startupsurvival.spark.FeatureJoin.CombinedFeatures

/**
 * PHASE 14/15 - Feature Sets
 *
 * Converts Phase 13's CombinedFeatures (one record per startup, financial
 * + sentiment halves) into the two DataFrames Spark MLlib's `ml` package
 * actually consumes: a `label` column (Double, required by
 * LogisticRegression/LinearSVC) and a `features` column (a dense
 * org.apache.spark.ml.linalg.Vector).
 *
 * Phase 14 (financial-only) feeds Experiments 1 and 2. Phase 15
 * (financial+sentiment) feeds Experiments 3 and 4. Both share the same
 * financial-feature encoding below, so Phase 15's vector is a strict
 * superset (financial features, unchanged, with sentiment features
 * appended) - this matters for a fair comparison between Experiments
 * 1-vs-3 and 2-vs-4: any accuracy difference is attributable to adding
 * sentiment, not to also re-encoding the financial half differently.
 *
 * ============================================================================
 * IMPUTATION STRATEGY - READ BEFORE CHANGING
 * ============================================================================
 * FinancialFeatures and SentimentFeatures both carry Option[_] fields for
 * values that are legitimately unknown (Phase 11's leakage guard nulling
 * totalFundingUsd/fundingRoundCount; Phase 10's zero-baseline for
 * no-coverage startups). Spark ML's Vector is a fixed-width array of
 * Doubles with no concept of "missing" - every field must become a
 * number.
 *
 * The approach here is: impute every None to 0.0, and ALWAYS pair a
 * potentially-imputed numeric field with its own explicit 0/1 indicator
 * feature (hasFundingAmountRecorded, fundingDataLeakageSafe are already
 * booleans from Phase 11; newsCount > 0 plays the same role for the
 * sentiment half, since every Option-valued sentiment field is None
 * exactly when newsCount == 0). This lets the model distinguish
 * "0 because leakage-unsafe/no coverage" from "0 because the true value
 * is genuinely 0" via the indicator, rather than silently conflating the
 * two - the same "never silently absorb a limitation" principle used
 * throughout this project (see Phase 2 R3's corrupted-date handling,
 * Phase 11's leakage guard itself).
 * ============================================================================
 */
object FeatureSets {

  /** Column order fixed here and documented so Evaluation.scala's
   * coefficient/importance printouts (Phase 18) can label features by
   * name rather than by bare index. */
  val FinancialFeatureNames: Array[String] = Array(
    "startupAgeYears",
    "totalFundingUsd",
    "fundingRoundCount",
    "averageRoundSizeUsd",
    "timeSinceLastFundingDays",
    "fundingFrequencyPerYear",
    "hasFundingAmountRecorded",
    "fundingDataLeakageSafe"
  )

  val SentimentFeatureNames: Array[String] = Array(
    "newsCount",
    "positiveNewsCount",
    "negativeNewsCount",
    "neutralNewsCount",
    "positiveRatio",
    "negativeRatio",
    "averageSentiment",
    "recentSentiment",
    "sentimentTrend",
    "sentimentVolatility",
    "newsVolumeTrend",
    "hasAnyCoverage" // = newsCount > 0, the indicator for every imputed-0 field above it
  )

  /** Financial half only - Phase 14. */
  def financialVector(cf: CombinedFeatures): Vector = {
    val f = cf.financial
    Vectors.dense(
      f.startupAgeYears,
      f.totalFundingUsd.getOrElse(0.0),
      f.fundingRoundCount.getOrElse(0).toDouble,
      f.averageRoundSizeUsd.getOrElse(0.0),
      f.timeSinceLastFundingDays.getOrElse(0L).toDouble,
      f.fundingFrequencyPerYear.getOrElse(0.0),
      if (f.hasFundingAmountRecorded) 1.0 else 0.0,
      if (f.fundingDataLeakageSafe) 1.0 else 0.0
    )
  }

  /** Sentiment half only - appended to the financial vector for Phase 15,
   * never used alone (Experiments 3/4 are "financial + sentiment", there
   * is no sentiment-only experiment in this project's design). */
  private def sentimentPart(cf: CombinedFeatures): Array[Double] = {
    val s = cf.sentiment
    Array(
      s.newsCount.toDouble,
      s.positiveNewsCount.toDouble,
      s.negativeNewsCount.toDouble,
      s.neutralNewsCount.toDouble,
      s.positiveRatio,
      s.negativeRatio,
      s.averageSentiment,
      s.recentSentiment.getOrElse(0.0),
      s.sentimentTrend.getOrElse(0.0),
      s.sentimentVolatility.getOrElse(0.0),
      s.newsVolumeTrend.getOrElse(0.0),
      if (s.newsCount > 0) 1.0 else 0.0
    )
  }

  /** Financial + sentiment, concatenated - Phase 15. */
  def combinedVector(cf: CombinedFeatures): Vector =
    Vectors.dense(financialVector(cf).toArray ++ sentimentPart(cf))

  private val Schema: StructType = StructType(Seq(
    StructField("permalink", StringType, nullable = false),
    StructField("label", DoubleType, nullable = false),
    StructField("features", SQLDataTypes.VectorType, nullable = false)
  ))

  /** Shared DataFrame-building step: RDD[(String, CombinedFeatures)] ->
   * DataFrame(permalink, label, features), given a vector-extraction
   * function. Both Phase 14 and Phase 15 are one-line callers of this. */
  private def toDataFrame(
      spark: SparkSession,
      combinedRDD: RDD[(String, CombinedFeatures)],
      vectorOf: CombinedFeatures => Vector
  ): DataFrame = {
    val rowRDD: RDD[Row] = combinedRDD.map { case (permalink, cf) =>
      Row(permalink, cf.label.toDouble, vectorOf(cf))
    }
    spark.createDataFrame(rowRDD, Schema)
  }

  /** PHASE 14 - financial-only feature set, feeding Experiments 1 (LR)
   * and 2 (SVM). Uses the FULL 22,075-startup population, since it never
   * depends on sentiment coverage. */
  def financialOnly(spark: SparkSession, combinedRDD: RDD[(String, CombinedFeatures)]): DataFrame =
    toDataFrame(spark, combinedRDD, financialVector).persist()

  /** PHASE 15 - financial+sentiment feature set, feeding Experiments 3
   * (LR) and 4 (SVM). Also uses the full population - every startup gets
   * a record via Phase 13's leftOuterJoin, real coverage or zero-baseline
   * alike - NOT filtered down to only the small real-coverage subset,
   * which would both shrink the dataset drastically and bias it toward
   * better-funded startups (Phase 7's coverage-correlates-with-funding
   * finding means "has real news" is itself informative about funding
   * size, so filtering to only those rows would leak that signal into
   * every experiment, not just Experiments 3/4). */
  def financialPlusSentiment(spark: SparkSession, combinedRDD: RDD[(String, CombinedFeatures)]): DataFrame =
    toDataFrame(spark, combinedRDD, combinedVector).persist()

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("FeatureSets-Phase14-15")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cutoff = LocalDate.of(2013, 1, 1)

    println("=== Rebuilding Phase 13's combined RDD ===")
    val startupRDD = StartupRDD.load(spark)
    val financialRDD = StartupRDD.extractFinancialFeaturesRDD(startupRDD)
    val newsRDD = NewsRDD.load(spark)
    val sentimentRDD = Aggregation.toSentimentFeaturesRDD(newsRDD.groupByKey(), cutoff)
    val combinedRDD = FeatureJoin.join(financialRDD, sentimentRDD, cutoff)

    println("\n=== PHASE 14: Financial-only feature set ===")
    val financialOnlyDF = financialOnly(spark, combinedRDD)
    println(s"Row count: ${financialOnlyDF.count()} (expect 22075)")
    println(s"Feature vector width: ${FinancialFeatureNames.length} (${FinancialFeatureNames.mkString(", ")})")
    financialOnlyDF.show(5, truncate = false)

    println("\n=== PHASE 15: Financial + sentiment feature set ===")
    val financialSentimentDF = financialPlusSentiment(spark, combinedRDD)
    println(s"Row count: ${financialSentimentDF.count()} (expect 22075)")
    println(s"Feature vector width: ${FinancialFeatureNames.length + SentimentFeatureNames.length} " +
      s"(${(FinancialFeatureNames ++ SentimentFeatureNames).mkString(", ")})")
    financialSentimentDF.show(5, truncate = false)

    spark.stop()
  }
}
