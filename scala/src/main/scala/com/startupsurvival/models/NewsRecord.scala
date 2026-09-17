package com.startupsurvival.models

import java.time.LocalDate

final case class NewsRecord(
    headline: String,
    publicationDate: Option[LocalDate],
    source: Option[String],
    url: Option[String],
    retrievalDate: LocalDate,
    matchedPermalink: Option[String],
    matchConfidence: Double,
    matchedStartupName: Option[String]
) {
  require(matchConfidence >= 0.0 && matchConfidence <= 1.0,
    s"matchConfidence must be in [0,1], got $matchConfidence")
}

final case class ScoredNewsRecord(
    news: NewsRecord,
    positiveMatches: Int,
    negativeMatches: Int,
    sentimentScore: Int,
    sentimentClass: SentimentClass
)

sealed trait SentimentClass
object SentimentClass {
  case object Positive extends SentimentClass
  case object Neutral extends SentimentClass
  case object Negative extends SentimentClass
}

final case class SentimentFeatures(
    permalink: String,
    newsCount: Int,
    positiveNewsCount: Int,
    negativeNewsCount: Int,
    neutralNewsCount: Int,
    positiveRatio: Double,
    negativeRatio: Double,
    averageSentiment: Double,
    recentSentiment: Option[Double],
    sentimentTrend: Option[Double],
    sentimentVolatility: Option[Double],
    newsVolumeTrend: Option[Double]
)
