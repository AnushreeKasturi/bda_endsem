package com.startupsurvival.spark

import java.time.LocalDate
import org.apache.spark.rdd.RDD
import com.startupsurvival.FeatureExtractor
import com.startupsurvival.models.{FinancialFeatures, SentimentFeatures}

/**
 * PHASE 13 - Feature Join
 *
 * Combines Phase 11's FinancialFeatures with Phase 10/12's
 * SentimentFeatures into one CombinedFeatures record per startup, keyed
 * by permalink, ready to feed both the Financial-only (Phase 14) and
 * Financial+Sentiment (Phase 15) pipelines.
 *
 * ============================================================================
 * WHY leftOuterJoin, NOT join() - THIS IS THE KEY DESIGN DECISION HERE
 * ============================================================================
 * Phase 12's NewsRDD.scala demonstrated plain join() as a mechanism
 * preview - an INNER join, which only keeps keys present in BOTH RDDs.
 * That was fine for a small preview (2 example companies), but it is the
 * WRONG join type for the real pipeline: most of the 22,075 startups have
 * no news coverage at all (Phase 7 found coverage correlates strongly
 * with funding size), so an inner join here would silently drop the vast
 * majority of the population - breaking Experiments 1/2 (Financial-only),
 * which need the FULL 22,075-startup population, not just the small
 * subset with news coverage.
 *
 * leftOuterJoin keeps every row from the left side (financialRDD, which
 * covers all 22,075 startups) and produces None on the right
 * (SentimentFeatures) for any startup with no match. Those None cases are
 * converted to a zero-baseline SentimentFeatures via
 * FeatureExtractor.extractSentimentFeatures(permalink, List.empty, cutoff)
 * - reusing the EXACT same empty-case logic already hand-verified in
 * Phase 10's "no-coverage startup" test, rather than duplicating those
 * zero values here and risking the two definitions drifting apart.
 * ============================================================================
 *
 * PERSISTENCE: the joined RDD is persisted here because it directly feeds
 * BOTH Phase 14 (Financial-only, which simply ignores the sentiment half)
 * and Phase 15 (Financial+Sentiment, which uses both halves) - exactly
 * the "combined feature dataset reused for multiple downstream pipelines"
 * scenario the project brief specifically calls out as needing
 * persistence.
 */
object FeatureJoin {

  final case class CombinedFeatures(
      permalink: String,
      label: Int,
      financial: FinancialFeatures,
      sentiment: SentimentFeatures
  )

  def join(
      financialRDD: RDD[(String, FinancialFeatures)],
      sentimentRDD: RDD[(String, SentimentFeatures)],
      cutoff: LocalDate
  ): RDD[(String, CombinedFeatures)] = {
    financialRDD
      .leftOuterJoin(sentimentRDD)
      .map { case (permalink, (financial, sentimentOpt)) =>
        val sentiment = sentimentOpt.getOrElse(
          FeatureExtractor.extractSentimentFeatures(permalink, List.empty, cutoff)
        )
        (permalink, CombinedFeatures(permalink, financial.label, financial, sentiment))
      }
      .persist() // reused by both Phase 14 and Phase 15 - see class comment
  }

  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession

    val spark = SparkSession.builder()
      .appName("FeatureJoin-Phase13")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val cutoff = LocalDate.of(2013, 1, 1)

    println("=== Building financial features from the REAL full HDFS dataset ===")
    val startupRDD = StartupRDD.load(spark)
    val financialRDD = StartupRDD.extractFinancialFeaturesRDD(startupRDD)
    val financialCount = financialRDD.count()
    println(s"Financial features built for: $financialCount startups (expect 22075)\n")

    println("=== Building sentiment features (currently only the small real")
    println("    Kabbage/Color Labs/Pinterest sample from Phase 7-13 - full batch")
    println("    collection has not been run yet, see docs/PHASE13_feature_join.md) ===")
    val newsRDD = NewsRDD.load(spark)
    val groupedNewsRDD = newsRDD.groupByKey()
    val sentimentRDD = Aggregation.toSentimentFeaturesRDD(groupedNewsRDD, cutoff)
    println(s"Sentiment features built for: ${sentimentRDD.count()} startups (expect 3)\n")

    println("=== Joining with leftOuterJoin (preserves the FULL population) ===")
    val combinedRDD = join(financialRDD, sentimentRDD, cutoff)
    val combinedCount = combinedRDD.count()
    println(s"Combined features: $combinedCount startups")
    println("Expect: 22075 - EVERY startup gets a combined record, unlike Phase 12's")
    println("inner-join preview which only returned the 2 startups present in both RDDs.\n")

    val withRealNews = combinedRDD.filter(_._2.sentiment.newsCount > 0).count()
    val withZeroBaseline = combinedCount - withRealNews
    println(s"Startups with real news coverage: $withRealNews")
    println(s"Startups with zero-baseline sentiment (no news collected yet): $withZeroBaseline")
    println("Expect: 3 with real coverage (Kabbage, Color Labs, Pinterest), 22072 on the")
    println("zero baseline - this ratio will change once full batch news collection runs.")

    println("\n=== Sample combined record (Kabbage - has real positive-tilted news) ===")
    combinedRDD.filter(_._1 == "/organization/kabbage").collect().foreach { case (_, cf) =>
      println(s"  label=${cf.label}")
      println(s"  financial: ageYears=${"%.1f".format(cf.financial.startupAgeYears)}, leakageSafe=${cf.financial.fundingDataLeakageSafe}")
      println(s"  sentiment: newsCount=${cf.sentiment.newsCount}, avgSentiment=${cf.sentiment.averageSentiment}")
    }

    println("\n=== Sample combined record (Color Labs - has real negative-tilted news) ===")
    combinedRDD.filter(_._1 == "/organization/color-labs").collect().foreach { case (_, cf) =>
      println(s"  label=${cf.label}")
      println(s"  financial: ageYears=${"%.1f".format(cf.financial.startupAgeYears)}, leakageSafe=${cf.financial.fundingDataLeakageSafe}")
      println(s"  sentiment: newsCount=${cf.sentiment.newsCount}, avgSentiment=${cf.sentiment.averageSentiment}")
    }

    println("\n=== Sample combined record (Pinterest - largest real coverage, 5 items) ===")
    combinedRDD.filter(_._1 == "/organization/pinterest").collect().foreach { case (_, cf) =>
      println(s"  label=${cf.label}")
      println(s"  financial: ageYears=${"%.1f".format(cf.financial.startupAgeYears)}, leakageSafe=${cf.financial.fundingDataLeakageSafe}")
      println(s"  sentiment: newsCount=${cf.sentiment.newsCount}, avgSentiment=${cf.sentiment.averageSentiment}")
    }

    println("\n=== Sample combined record (1-800-DOCTORS - REAL eligible startup,")
    println("    genuinely zero news coverage after name-matching, not a made-up case) ===")
    combinedRDD.filter(_._1 == "/organization/1-800-doctors").collect().foreach { case (_, cf) =>
      println(s"  label=${cf.label}")
      println(s"  financial: ageYears=${"%.1f".format(cf.financial.startupAgeYears)}, leakageSafe=${cf.financial.fundingDataLeakageSafe}")
      println(s"  sentiment: newsCount=${cf.sentiment.newsCount} (zero-baseline via leftOuterJoin's None branch)")
    }

    spark.stop()
  }
}
