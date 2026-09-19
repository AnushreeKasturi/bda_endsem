package com.startupsurvival.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import java.time.LocalDate
import com.startupsurvival.SentimentScorer
import com.startupsurvival.models.{NewsRecord, ScoredNewsRecord}

/**
 * PHASE 12 - Spark RDD Pipeline: News RDD
 *
 * A full-scale batch news collection across all 22,075 startups has NOT
 * been run yet (Phase 7 was a manual feasibility spike against 3
 * companies, not a bulk job - see docs/PHASE07_news_collection.md). That
 * batch run is a prerequisite for Phase 13's real join at full scale, and
 * is flagged as the next concrete action after this phase, not silently
 * assumed to already exist.
 *
 * This file demonstrates the News RDD's key/value shape and its core
 * transformations against the same REAL data already verified in
 * Phase 10 - the actual Kabbage and Sifteo headlines, dates, and scores
 * captured live from Google News RSS and scored with the real
 * SentimentScorer. Small in volume, but every value in it is genuine,
 * not synthetic - and the operations shown here (parallelize, map,
 * reduceByKey, groupByKey, join) are exactly what will run against the
 * full batch-collected dataset once it exists.
 *
 * RDD CREATION VIA parallelize(): unlike StartupRDD's sc.textFile()
 * (creating an RDD from an external file), this file demonstrates the
 * OTHER standard RDD creation method - sc.parallelize() distributes an
 * existing in-driver Scala collection across the cluster's partitions.
 * Both are valid RDD creation methods; which one is appropriate depends
 * on whether the source data already lives in a file (textFile) or is
 * already a Scala collection in the driver's memory (parallelize).
 */
object NewsRDD {

  /** Builds the same real ScoredNewsRecord data verified in Phase 10,
   * this time as the seed for an RDD rather than a plain Scala List. */
  private def realSampleNews(): List[(String, ScoredNewsRecord)] = {
    def scored(permalink: String, headline: String, dateStr: String): (String, ScoredNewsRecord) = {
      val news = NewsRecord(
        headline = headline,
        publicationDate = Some(LocalDate.parse(dateStr)),
        source = None, url = None,
        retrievalDate = LocalDate.now(),
        matchedPermalink = Some(permalink),
        matchConfidence = 1.0,
        matchedStartupName = None
      )
      (permalink, SentimentScorer.scoreNews(news))
    }

    List(
      scored("/organization/kabbage", "Kabbage Raises Some Serious Cabbage for Small-Business Loans - WIRED", "2012-09-19"),
      scored("/organization/kabbage", "Kabbage: The Merchant Cash Advance of the Online Business World - deBanked", "2011-08-23"),
      scored("/organization/sifteo", "Sifteo Cubes Are Out Today, And Even Better Than You Imagined - Fast Company", "2011-08-11"),
      scored("/organization/sifteo", "Tactile Digital Play, Part 1: Sifteo Cubes and Hasbro Zapped Toys - WIRED", "2012-10-27"),
      scored("/organization/sifteo", "First Look: New Sifteo Cubes Go Gaming - NBC Bay Area", "2012-08-30"),
      scored("/organization/sifteo", "Review: Sifteo Cubes bring physicality back to digital games - Ars Technica", "2011-08-10")
    )
  }

  /** RDD CREATION via parallelize - distributes the real sample data
   * (built on the driver) into a key/value RDD across the cluster. */
  def load(spark: SparkSession): RDD[(String, ScoredNewsRecord)] =
    spark.sparkContext.parallelize(realSampleNews())

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NewsRDD-Phase12")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    println("=== Loading News RDD (real Phase 7/9/10 data, via parallelize) ===")
    val newsRDD = load(spark).persist()

    val total = newsRDD.count()
    println(s"Total scored news items: $total (expect 6: 2 Kabbage + 4 Sifteo)\n")

    // reduceByKey: aggregate sentiment score SUM per startup. Demonstrates
    // combining values for the same key across partitions - the values
    // being combined here are just the sentimentScore Ints, extracted via
    // an initial map.
    println("=== Total sentiment score per startup (reduceByKey) ===")
    val scoreSumRDD: RDD[(String, Int)] =
      newsRDD
        .map { case (permalink, scored) => (permalink, scored.sentimentScore) }
        .reduceByKey(_ + _)
    scoreSumRDD.collect().sorted.foreach { case (permalink, sum) =>
      println(s"  $permalink -> sentiment sum = $sum")
    }
    println("Expect: kabbage -> 1 (scores were +1 and 0), sifteo -> 0 (all four scored 0)\n")

    // groupByKey: used here deliberately, not as a default choice. It is
    // justified in this specific case because we need the FULL LIST of a
    // startup's ScoredNewsRecords together (to compute Phase 10's
    // chronological trend/volatility features, which need every
    // individual record, not just a running sum) - reduceByKey/
    // aggregateByKey only combine down to a single accumulated value and
    // cannot reconstruct the original list of records afterward.
    // groupByKey is more expensive (it shuffles every individual record
    // rather than pre-combining on each partition first), so it is used
    // ONLY where the full grouped list is genuinely needed, not as a
    // default aggregation tool - this is exactly the "use groupByKey
    // where justified" and "avoid inefficient operations just to show
    // them" balance the project brief asks for.
    println("=== Full record groups per startup (groupByKey, used because Phase 10's")
    println("    FeatureExtractor needs every individual record, not a running total) ===")
    val groupedRDD: RDD[(String, Iterable[ScoredNewsRecord])] = newsRDD.groupByKey()
    groupedRDD.collect().sortBy(_._1).foreach { case (permalink, records) =>
      println(s"  $permalink -> ${records.size} records")
    }

    // JOIN PREVIEW: joins the small news sample against the REAL, full
    // StartupRDD from HDFS, by permalink. This is a genuine rehearsal of
    // Phase 13's full-scale join, not a mock - StartupRDD.load() below
    // reads the actual 22,075-row HDFS file.
    println("\n=== Join preview: news RDD joined against the REAL StartupRDD ===")
    val startupRDD = StartupRDD.load(spark)
    val joined: RDD[(String, (Iterable[ScoredNewsRecord], com.startupsurvival.models.StartupRecord))] =
      groupedRDD.join(startupRDD)
    joined.collect().sortBy(_._1).foreach { case (permalink, (newsGroup, startup)) =>
      println(s"  $permalink (label=${startup.label}) matched with ${newsGroup.size} news records")
    }
    println("\nNote: join() only returns keys present in BOTH RDDs (an inner join) -")
    println("since groupedRDD only has 2 permalinks, only those 2 appear here, even")
    println(s"though startupRDD has all ${startupRDD.count()} real startups. Phase 13's full")
    println("join will behave identically at full scale once batch news collection runs.")

    spark.stop()
  }
}
