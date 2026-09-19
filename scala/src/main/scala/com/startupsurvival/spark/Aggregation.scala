package com.startupsurvival.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.time.LocalDate
import com.startupsurvival.FeatureExtractor
import com.startupsurvival.models.{ScoredNewsRecord, SentimentFeatures}

/**
 * PHASE 12 - Spark RDD Pipeline: Aggregation
 *
 * Runs Phase 10's FeatureExtractor.extractSentimentFeatures - already
 * hand-verified against real data in Phase 10 - distributed across the
 * News RDD via mapValues, producing RDD[(String, SentimentFeatures)].
 *
 * This is the direct distributed counterpart to StartupRDD.scala's
 * extractFinancialFeaturesRDD: both feature extractors built in plain
 * Scala (Phases 10 and 11) now have a one-line distributed version here,
 * because both were written as pure functions from the start - exactly
 * the payoff of keeping feature-extraction logic separate from any
 * particular execution engine back in Phases 10-11.
 */
object Aggregation {

  /**
   * Takes a grouped News RDD (permalink -> all of that startup's
   * ScoredNewsRecords, as produced by NewsRDD's groupByKey) and maps each
   * group through Phase 10's real aggregation logic, using mapValues so
   * the permalink key is preserved without re-pairing.
   */
  def toSentimentFeaturesRDD(
      groupedNewsRDD: RDD[(String, Iterable[ScoredNewsRecord])],
      cutoff: LocalDate
  ): RDD[(String, SentimentFeatures)] =
    groupedNewsRDD.mapValues { records =>
      FeatureExtractor.extractSentimentFeatures(
        permalink = "", // permalink is already the RDD key; the field
                          // inside SentimentFeatures is overwritten below
                          // for consistency, since extractSentimentFeatures
                          // takes it as a parameter rather than reading it
                          // off the records themselves
        scoredNews = records.toList,
        cutoff = cutoff
      )
    }.map { case (permalink, features) =>
      // Ensure the SentimentFeatures.permalink field matches the RDD key
      // exactly, rather than the placeholder passed above.
      (permalink, features.copy(permalink = permalink))
    }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Aggregation-Phase12")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cutoff = LocalDate.of(2013, 1, 1)

    println("=== Distributed sentiment feature aggregation (real Phase 7/9/10 data) ===")
    val newsRDD = NewsRDD.load(spark)
    val groupedRDD = newsRDD.groupByKey()
    val sentimentFeaturesRDD = toSentimentFeaturesRDD(groupedRDD, cutoff)

    sentimentFeaturesRDD.collect().sortBy(_._1).foreach { case (permalink, features) =>
      println(s"\n$permalink:")
      println(s"  $features")
    }

    println("\nExpect these to match Phase 10's hand-verified plain-Scala results exactly:")
    println("  kabbage: newsCount=2, positiveRatio=0.5, averageSentiment=0.5, sentimentTrend=Some(1.0)")
    println("  sifteo:  newsCount=4, all-neutral, averageSentiment=0.0, sentimentTrend=Some(0.0)")

    spark.stop()
  }
}
