package com.startupsurvival

import java.time.LocalDate
import com.startupsurvival.models.{ScoredNewsRecord, SentimentClass, SentimentFeatures}

/**
 * PHASE 10 - Sentiment Feature Engineering
 *
 * Aggregates a startup's ScoredNewsRecords (Phases 7-9 output) into the
 * SentimentFeatures case class defined in Phase 3, ready to join onto a
 * StartupRecord by permalink in Phase 13.
 *
 * LEAKAGE GUARD, RE-ENFORCED HERE: NewsCollector (Phase 7) already filters
 * to publicationDate < cutoff before returning items, but this file
 * re-applies that same filter before computing anything. This matches the
 * project's established defensive pattern (see StartupRecord's own
 * require() guard, Phase 3) - never trust an earlier stage's filter alone
 * when a later stage can cheaply re-verify it, since a single missed
 * filter anywhere in the chain would silently reintroduce the exact
 * temporal leakage the whole project is designed to avoid.
 *
 * Every field below matches a feature named in the project brief. Each
 * has its formula and reasoning stated explicitly, per the brief's
 * requirement to "explain each feature."
 */
object FeatureExtractor {

  /**
   * Builds a startup's complete SentimentFeatures record from its scored
   * news items. If scoredNews is empty (or entirely post-cutoff after
   * re-filtering), returns a well-defined zero/neutral baseline rather
   * than nulls - a startup with no discoverable news coverage is
   * legitimate, expected data (see Phase 7's finding that coverage
   * correlates with funding size), not missing data to error on.
   */
  def extractSentimentFeatures(
      permalink: String,
      scoredNews: List[ScoredNewsRecord],
      cutoff: LocalDate
  ): SentimentFeatures = {

    // Re-enforced leakage guard - see class-level comment.
    val eligible = scoredNews.filter { s =>
      s.news.publicationDate.exists(_.isBefore(cutoff))
    }

    val newsCount = eligible.length

    if (newsCount == 0) {
      return SentimentFeatures(
        permalink = permalink,
        newsCount = 0,
        positiveNewsCount = 0,
        negativeNewsCount = 0,
        neutralNewsCount = 0,
        positiveRatio = 0.0,
        negativeRatio = 0.0,
        averageSentiment = 0.0,
        recentSentiment = None,
        sentimentTrend = None,
        sentimentVolatility = None,
        newsVolumeTrend = None
      )
    }

    // --- Basic counts: news_count, positive/negative/neutral_news_count ---
    // positive/negative/neutral_news_count: straightforward counts of each
    // sentimentClass across the eligible news, using count() with pattern
    // matching on the sealed SentimentClass trait (Phase 3).
    val positiveNewsCount = eligible.count(_.sentimentClass == SentimentClass.Positive)
    val negativeNewsCount = eligible.count(_.sentimentClass == SentimentClass.Negative)
    val neutralNewsCount  = eligible.count(_.sentimentClass == SentimentClass.Neutral)

    // positive_ratio / negative_ratio: share of a startup's coverage that
    // is positive/negative, independent of raw volume - lets the model
    // distinguish "1 article, positive" from "50 articles, 1 positive"
    // rather than conflating both as "some positive news."
    val positiveRatio = positiveNewsCount.toDouble / newsCount
    val negativeRatio = negativeNewsCount.toDouble / newsCount

    // average_sentiment: mean of the raw per-article sentiment_score
    // (Phase 9's positive_matches - negative_matches), giving an overall
    // tone measure that a simple positive/negative count alone cannot -
    // e.g. distinguishes mildly vs strongly positive coverage.
    val averageSentiment =
      eligible.map(_.sentimentScore).sum.toDouble / newsCount

    // Chronological ordering is required for recent_sentiment,
    // sentiment_trend, and news_volume_trend below. Safe to call .get on
    // publicationDate here - the eligibility filter above guarantees it
    // is defined for every item in `eligible`.
    val sorted = eligible.sortBy(_.news.publicationDate.get.toEpochDay)

    // recent_sentiment: the sentiment_score of the single most recent
    // pre-cutoff article. Distinct from average_sentiment - a startup
    // with historically positive coverage that just turned negative right
    // before the cutoff is a meaningfully different signal than one whose
    // negative coverage was old and has since improved; average_sentiment
    // alone cannot distinguish these two cases, recent_sentiment can.
    val recentSentiment: Option[Double] =
      Some(sorted.last.sentimentScore.toDouble)

    // sentiment_trend: average sentiment of the chronologically later half
    // of a startup's coverage minus the average of the earlier half.
    // Positive => sentiment improving over time; negative => declining.
    // Requires at least 2 articles to have two non-empty halves; None
    // otherwise (a trend needs at least two points to be meaningful).
    val sentimentTrend: Option[Double] =
      if (newsCount >= 2) {
        val mid = newsCount / 2
        val firstHalf = sorted.take(mid)
        val secondHalf = sorted.drop(mid)
        val firstAvg = firstHalf.map(_.sentimentScore).sum.toDouble / firstHalf.length
        val secondAvg = secondHalf.map(_.sentimentScore).sum.toDouble / secondHalf.length
        Some(secondAvg - firstAvg)
      } else None

    // sentiment_volatility: sample standard deviation of sentiment_score
    // across all eligible articles. High volatility means a startup's
    // coverage swings between positive and negative rather than being
    // consistently one or the other - potentially informative on its own,
    // independent of the average. Requires n >= 2 (sample stdev is
    // undefined for a single point).
    val sentimentVolatility: Option[Double] =
      if (newsCount >= 2) {
        val mean = averageSentiment
        val sumSquaredDiffs = eligible
          .map(s => math.pow(s.sentimentScore.toDouble - mean, 2))
          .sum
        Some(math.sqrt(sumSquaredDiffs / (newsCount - 1)))
      } else None

    // news_volume_trend: article count in the more-recent half of the
    // lookback window (cutoff minus 12 months, to cutoff) minus the count
    // in the earlier half (cutoff minus 24 months, to cutoff minus 12
    // months) - matching NewsCollector's 24-month lookback window
    // (Phase 7). Positive => coverage accelerating toward the cutoff;
    // negative => coverage tapering off. Computed whenever any news
    // exists (newsCount >= 1), since even a single-bucket result (e.g.
    // all coverage recent, none in the earlier half) is meaningful, not
    // missing data.
    val windowMidpoint = cutoff.minusMonths(12)
    val recentBucketCount = eligible.count { s =>
      s.news.publicationDate.exists(d => !d.isBefore(windowMidpoint))
    }
    val earlyBucketCount = newsCount - recentBucketCount
    val newsVolumeTrend: Option[Double] =
      Some((recentBucketCount - earlyBucketCount).toDouble)

    SentimentFeatures(
      permalink = permalink,
      newsCount = newsCount,
      positiveNewsCount = positiveNewsCount,
      negativeNewsCount = negativeNewsCount,
      neutralNewsCount = neutralNewsCount,
      positiveRatio = positiveRatio,
      negativeRatio = negativeRatio,
      averageSentiment = averageSentiment,
      recentSentiment = recentSentiment,
      sentimentTrend = sentimentTrend,
      sentimentVolatility = sentimentVolatility,
      newsVolumeTrend = newsVolumeTrend
    )
  }

  def main(args: Array[String]): Unit = {
    val cutoff = LocalDate.of(2013, 1, 1)

    // Real headlines and real dates captured live in Phase 7, filtered
    // down to exactly the items Phase 8's NameMatcher actually accepted
    // (2 of 5 for Kabbage, 4 of 5 for Sifteo - see docs/PHASE08_name_matching.md),
    // then scored with Phase 9's real SentimentScorer. This is a genuine
    // cross-phase integration test, not synthetic data.
    def buildScored(headline: String, dateStr: String): ScoredNewsRecord = {
      val date = LocalDate.parse(dateStr)
      val news = com.startupsurvival.models.NewsRecord(
        headline = headline,
        publicationDate = Some(date),
        source = None,
        url = None,
        retrievalDate = LocalDate.now(),
        matchedPermalink = Some("/organization/test"),
        matchConfidence = 1.0,
        matchedStartupName = Some("Test")
      )
      SentimentScorer.scoreNews(news)
    }

    val kabbageNews = List(
      buildScored("Kabbage Raises Some Serious Cabbage for Small-Business Loans - WIRED", "2012-09-19"),
      buildScored("Kabbage: The Merchant Cash Advance of the Online Business World - deBanked", "2011-08-23")
    )

    val sifteoNews = List(
      buildScored("Sifteo Cubes Are Out Today, And Even Better Than You Imagined - Fast Company", "2011-08-11"),
      buildScored("Tactile Digital Play, Part 1: Sifteo Cubes and Hasbro Zapped Toys - WIRED", "2012-10-27"),
      buildScored("First Look: New Sifteo Cubes Go Gaming - NBC Bay Area", "2012-08-30"),
      buildScored("Review: Sifteo Cubes bring physicality back to digital games - Ars Technica", "2011-08-10")
    )
    // NOTE (Phase 13 cleanup round 2): Sifteo was found to be genuinely
    // ineligible (acquired by 3D Robotics in 2014; excluded under Phase
    // 2's R2 rule - confirmed absent from labeled_eligible_startups.csv
    // via grep). Its name-matching/sentiment-scoring results above remain
    // valid as a Phase 9/10 demonstration (eligibility was never required
    // for that), but it is NOT used in Phase 13's join demo - see
    // NewsRDD.scala and docs/PHASE13_feature_join.md, where Pinterest
    // replaces it.

    // Color Labs - real live NewsCollector run added in the Phase 13
    // cleanup (see docs/PHASE13_feature_join.md), replacing the old
    // Instructure placeholder that had no real headline text backing it.
    // Real, eligible startup: $41M funding, label=0 (closed). 4 of 5
    // returned items accepted by NameMatcher; 1 rejected as noise.
    val colorLabsNews = List(
      buildScored("$41 million can't buy success as Color app finally gives up (update: Color denies shutdown) - The Verge", "2012-10-17"),
      buildScored("A Mess Of Family Dynamics Alleged In Lawsuit Against Silicon Valley Entrepreneur And Color Founder Bill Nguyen - Forbes", "2012-11-20"),
      buildScored("Apple to acquire troubled startup Color Labs? - Gadgets 360", "2012-10-18"),
      buildScored("Exploring The \"Labs\" Trend in Consumer Startups - TechCrunch", "2011-12-04")
    )

    // Pinterest - real live NewsCollector run (Phase 13 cleanup round 2),
    // replacing Sifteo in the join-eligible sample. Real, eligible
    // startup: $1.3B funding, label=1 (operating), never acquired. 100
    // pre-cutoff items found; first 5 (NewsCollector's display cap) all
    // accepted at confidence 1.0 - "Pinterest" clears the isAmbiguousName
    // length guard, including one generic listicle headline that isn't
    // really company news, a documented NameMatcher limitation.
    val pinterestNews = List(
      buildScored("Ben Silbermann On How Pinterest Slowly Grew To Massive Scale - Forbes", "2012-10-22"),
      buildScored("INSIDE PINTEREST: An Overnight Success Four Years In The Making - Business Insider", "2012-05-01"),
      buildScored("42 Awesome and Creative Pinterest Boards - matadornetwork.com", "2012-01-31"),
      buildScored("Pinterest, Tumblr and the Trouble With \u2018Curation\u2019 (Published 2012) - The New York Times", "2012-07-20"),
      buildScored("The Pinterest Pivot - Fast Company", "2012-10-23")
    )

    // 1-800-DOCTORS - a REAL, eligible startup (label=1, $1.75M funding,
    // confirmed via grep against labeled_eligible_startups.csv in the
    // Phase 13 cleanup) whose live NewsCollector run returned 3 items,
    // ALL correctly rejected by NameMatcher (0/3 token overlap - generic
    // hospital press-release noise, not genuinely about the company).
    // This is a real, not synthetic, verification of the zero-coverage
    // baseline path - distinct from a made-up placeholder permalink.
    val emptyNews = List.empty[ScoredNewsRecord]

    println("=== Kabbage sentiment features (2 real accepted articles) ===")
    val kabbageFeatures = extractSentimentFeatures("/organization/kabbage", kabbageNews, cutoff)
    println(kabbageFeatures)

    println("\n=== Sifteo sentiment features (4 real accepted articles - Phase 9/10")
    println("    demonstration only; NOT part of Phase 13's join, see note above) ===")
    val sifteoFeatures = extractSentimentFeatures("/organization/sifteo", sifteoNews, cutoff)
    println(sifteoFeatures)

    println("\n=== Color Labs sentiment features (4 real accepted articles) ===")
    val colorLabsFeatures = extractSentimentFeatures("/organization/color-labs", colorLabsNews, cutoff)
    println(colorLabsFeatures)

    println("\n=== Pinterest sentiment features (5 real accepted articles) ===")
    val pinterestFeatures = extractSentimentFeatures("/organization/pinterest", pinterestNews, cutoff)
    println(pinterestFeatures)

    println("\n=== 1-800-DOCTORS: real eligible startup, real zero-coverage case ===")
    val emptyFeatures = extractSentimentFeatures("/organization/1-800-doctors", emptyNews, cutoff)
    println(emptyFeatures)
  }
}
