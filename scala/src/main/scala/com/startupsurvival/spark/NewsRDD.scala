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
 * Phase 10 - the actual Kabbage, Sifteo, and Color Labs headlines,
 * dates, and scores captured live from Google News RSS and scored with
 * the real SentimentScorer. Small in volume, but every value in it is
 * genuine, not synthetic - and the operations shown here (parallelize,
 * map, reduceByKey, groupByKey, join) are exactly what will run against
 * the full batch-collected dataset once it exists.
 *
 * PROVENANCE NOTE (Phase 13 cleanup, round 2): Sifteo was removed from
 * this join-demo sample. It is NOT a bug fix for a slug typo - Sifteo
 * was genuinely never eligible under Phase 2's R2 rule (acquired by 3D
 * Robotics in 2014, and `acquired` status is excluded). Its absence from
 * `labeled_eligible_startups.csv` was confirmed via grep. Sifteo's real
 * headline/sentiment data remains valid and unchanged in
 * FeatureExtractor.scala/SentimentScorer.scala as a Phase 9/10
 * name-matching and scoring example (those phases never required
 * eligibility), but it cannot correctly participate in this file's
 * full-population join demo, since leftOuterJoin only preserves rows
 * keyed by the eligible financial side - Sifteo's news data would
 * otherwise silently vanish from combinedRDD rather than landing on
 * either the real-coverage or zero-baseline branch. Pinterest (grep-
 * confirmed eligible, $1.3B funding, label=1, never acquired) replaces
 * it here.
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
      // Color Labs - real live NewsCollector run (Phase 13 cleanup), name-
      // matched via NameMatcher against "Color Labs" (see docs/PHASE13_feature_join.md).
      // 4 of 5 returned items accepted; 1 rejected (an unrelated obituary,
      // 0/2 token overlap - the same kind of clean noise rejection as
      // Kabbage's "tomato barons" case in Phase 8).
      scored("/organization/color-labs", "$41 million can't buy success as Color app finally gives up (update: Color denies shutdown) - The Verge", "2012-10-17"),
      scored("/organization/color-labs", "A Mess Of Family Dynamics Alleged In Lawsuit Against Silicon Valley Entrepreneur And Color Founder Bill Nguyen - Forbes", "2012-11-20"),
      scored("/organization/color-labs", "Apple to acquire troubled startup Color Labs? - Gadgets 360", "2012-10-18"),
      scored("/organization/color-labs", "Exploring The \"Labs\" Trend in Consumer Startups - TechCrunch", "2011-12-04"),
      // Pinterest - real live NewsCollector run (Phase 13 cleanup round 2,
      // replacing the never-eligible Sifteo - see provenance note above).
      // 100 pre-cutoff items found; first 5 (NewsCollector's spike-run
      // display cap) all accepted by NameMatcher at confidence 1.0, since
      // "Pinterest" (9 chars) clears the isAmbiguousName length guard -
      // including one generic listicle ("42 Awesome ... Pinterest
      // Boards") that isn't really company news, a real, documented
      // limitation distinct from the short-name ambiguity case.
      scored("/organization/pinterest", "Ben Silbermann On How Pinterest Slowly Grew To Massive Scale - Forbes", "2012-10-22"),
      scored("/organization/pinterest", "INSIDE PINTEREST: An Overnight Success Four Years In The Making - Business Insider", "2012-05-01"),
      scored("/organization/pinterest", "42 Awesome and Creative Pinterest Boards - matadornetwork.com", "2012-01-31"),
      scored("/organization/pinterest", "Pinterest, Tumblr and the Trouble With \u2018Curation\u2019 (Published 2012) - The New York Times", "2012-07-20"),
      scored("/organization/pinterest", "The Pinterest Pivot - Fast Company", "2012-10-23")
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
    println(s"Total scored news items: $total (expect 11: 2 Kabbage + 4 Color Labs + 5 Pinterest)\n")

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
    println("Expect: kabbage -> 1 (scores were +1 and 0), color-labs -> -2 (scores were")
    println("0, -1, -1, 0), pinterest -> 2 (scores were +2, +1, 0, -1, 0)\n")

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
    println("since groupedRDD only has 3 permalinks, only those 3 appear here, even")
    println(s"though startupRDD has all ${startupRDD.count()} real startups. Phase 13's full")
    println("join will behave identically at full scale once batch news collection runs.")

    spark.stop()
  }
}
