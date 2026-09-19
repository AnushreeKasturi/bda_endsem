package com.startupsurvival.models

/**
 * PHASE 11 - Group A (financial/structured) feature vector for one
 * startup, computed from its StartupRecord (Phase 3/6).
 *
 * IMPORTANT - read fundingDataLeakageSafe before using totalFundingUsd or
 * fundingRoundCount anywhere: see FinancialFeatureExtractor.scala for the
 * full explanation. In short, the raw dataset's funding totals and round
 * counts are LIFETIME figures (as of the ~2014/2015 Crunchbase snapshot),
 * not pre-cutoff-only figures. totalFundingUsd and fundingRoundCount are
 * only populated here (Some(...)) when it is logically guaranteed safe to
 * do so; otherwise they are None, and fundingDataLeakageSafe = false
 * explains why.
 */
final case class FinancialFeatures(
    permalink: String,
    label: Int,
    startupAgeYears: Double,
    totalFundingUsd: Option[Double],
    fundingRoundCount: Option[Int],
    averageRoundSizeUsd: Option[Double],
    timeSinceLastFundingDays: Option[Long],
    fundingFrequencyPerYear: Option[Double],
    hasFundingAmountRecorded: Boolean,
    fundingDataLeakageSafe: Boolean,
    categoryList: Option[String],
    countryCode: Option[String]
)
