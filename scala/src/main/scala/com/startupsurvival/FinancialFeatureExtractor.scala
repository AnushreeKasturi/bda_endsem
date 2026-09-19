package com.startupsurvival

import java.time.temporal.ChronoUnit
import com.startupsurvival.models.{StartupRecord, FinancialFeatures}

/**
 * PHASE 11 - Financial/Startup Feature Engineering (Group A features)
 *
 * Builds the structured feature vector used in Experiments 1 and 2
 * (financial-only) and, combined with Phase 10's SentimentFeatures, in
 * Experiments 3 and 4.
 *
 * ============================================================================
 * THE LEAKAGE GUARD - READ THIS BEFORE MODIFYING totalFundingUsd/
 * fundingRoundCount LOGIC BELOW
 * ============================================================================
 * StartupRecord.fundingTotalUsd and .fundingRounds are LIFETIME totals as
 * recorded in the raw Crunchbase snapshot (which extends to ~2014/2015 -
 * see docs/PHASE01_dataset_analysis.md), not pre-cutoff-only figures.
 * Phase 2's R4 rule only nulled lastFundingAt when it fell on/after the
 * cutoff; it did NOT adjust fundingTotalUsd or fundingRounds themselves.
 * Using either directly as a feature risks real temporal leakage: a
 * startup could show "5 rounds, $50M" even if 3 rounds and $30M of that
 * happened after the prediction cutoff.
 *
 * The fix used here relies on a clean logical guarantee: lastFundingAt is
 * a MAXIMUM (the date of the most recent funding event). If
 * lastFundingAt is defined (i.e. it survived Phase 2's nulling, meaning
 * it is confirmed < cutoff), then EVERY funding event for that startup -
 * by definition, since none can exceed the maximum - happened before the
 * cutoff. In that case, and only in that case, fundingTotalUsd and
 * fundingRounds are safe to use as-is.
 *
 * If lastFundingAt is None (either genuinely missing, or nulled because
 * it was >= cutoff), we cannot determine how much of the recorded total/
 * round-count happened pre-cutoff without per-round dates, which this
 * dataset does not provide. In that case totalFundingUsd and
 * fundingRoundCount are both set to None here - explicitly marked
 * unusable rather than silently leaked.
 * ============================================================================
 *
 * FEATURES NOT COMPUTED, AND WHY:
 *   - investor_count / unique_investor_count: requires per-round investor
 *     data, not present in the primary dataset (only available via the
 *     optional chhinna enrichment flagged in Phase 1, deliberately
 *     deferred for scope reasons documented there).
 *   - largest_round: requires per-round amounts; only aggregate lifetime
 *     total and round count are available, not a breakdown.
 * Stated explicitly here and in docs/PHASE11_financial_features.md rather
 * than fabricating placeholder values for either.
 */
object FinancialFeatureExtractor {

  private val DaysPerYear = 365.25

  def extractFinancialFeatures(startup: StartupRecord): FinancialFeatures = {
    val ageDays = ChronoUnit.DAYS.between(startup.foundedAt, startup.predictionCutoff)
    val ageYears = ageDays.toDouble / DaysPerYear

    // The leakage guard - see class-level comment.
    val leakageSafe = startup.lastFundingAt.isDefined

    val totalFundingUsd: Option[Double] =
      if (leakageSafe) startup.fundingTotalUsd else None

    val fundingRoundCount: Option[Int] =
      if (leakageSafe) startup.fundingRounds else None

    // average_round_size: only computable when both the total and a
    // positive round count are available and leakage-safe.
    val averageRoundSizeUsd: Option[Double] =
      for {
        total <- totalFundingUsd
        rounds <- fundingRoundCount if rounds > 0
      } yield total / rounds

    // time_since_last_funding: only defined when lastFundingAt itself is
    // defined (which, per the guard above, also means leakage-safe).
    val timeSinceLastFundingDays: Option[Long] =
      startup.lastFundingAt.map(d => ChronoUnit.DAYS.between(d, startup.predictionCutoff))

    // funding_frequency: rounds per year of the startup's pre-cutoff age.
    // Guards against division by a near-zero age for a startup founded
    // very close to the cutoff itself.
    val fundingFrequencyPerYear: Option[Double] =
      fundingRoundCount.map { rounds =>
        val safeAgeYears = math.max(ageYears, 1.0 / DaysPerYear)
        rounds.toDouble / safeAgeYears
      }

    // Diagnostic flag, independent of leakage-safety: did the ORIGINAL
    // record have any funding amount at all (i.e. was it not the "-"
    // placeholder found in Phase 1/6)? Useful for understanding missing-
    // data patterns even for startups whose amount we can't trust as
    // pre-cutoff-only.
    val hasFundingAmountRecorded = startup.fundingTotalUsd.isDefined

    FinancialFeatures(
      permalink = startup.permalink,
      label = startup.label,
      startupAgeYears = ageYears,
      totalFundingUsd = totalFundingUsd,
      fundingRoundCount = fundingRoundCount,
      averageRoundSizeUsd = averageRoundSizeUsd,
      timeSinceLastFundingDays = timeSinceLastFundingDays,
      fundingFrequencyPerYear = fundingFrequencyPerYear,
      hasFundingAmountRecorded = hasFundingAmountRecorded,
      fundingDataLeakageSafe = leakageSafe,
      categoryList = startup.categoryList,
      countryCode = startup.countryCode
    )
  }

  def main(args: Array[String]): Unit = {
    import java.time.LocalDate

    val cutoff = LocalDate.of(2013, 1, 1)

    // Case A: leakage-safe. lastFundingAt is defined and < cutoff, so all
    // funding activity is guaranteed pre-cutoff - totalFunding/roundCount
    // should be USED as-is.
    val safeStartup = StartupRecord(
      permalink = "/organization/safe-example",
      name = "Safe Example",
      homepageUrl = None, categoryList = Some("Software"),
      countryCode = Some("USA"), stateCode = None, region = None, city = None,
      fundingTotalUsd = Some(5000000.0),
      fundingRounds = Some(3),
      foundedAt = LocalDate.of(2008, 1, 1),
      firstFundingAt = LocalDate.of(2009, 1, 1),
      lastFundingAt = Some(LocalDate.of(2012, 6, 1)),
      label = 1,
      predictionCutoff = cutoff
    )

    // Case B: NOT leakage-safe. lastFundingAt is None (was nulled by
    // Phase 2 R4 because the startup's real last funding event was on/
    // after the cutoff, or genuinely missing) - totalFunding/roundCount
    // must be treated as unusable, even though the raw fields have values.
    val unsafeStartup = StartupRecord(
      permalink = "/organization/unsafe-example",
      name = "Unsafe Example",
      homepageUrl = None, categoryList = Some("Biotech"),
      countryCode = Some("USA"), stateCode = None, region = None, city = None,
      fundingTotalUsd = Some(50000000.0), // lifetime total - may include post-cutoff rounds!
      fundingRounds = Some(5),             // same risk
      foundedAt = LocalDate.of(2005, 1, 1),
      firstFundingAt = LocalDate.of(2006, 1, 1),
      lastFundingAt = None, // nulled - last real funding event was >= cutoff
      label = 1,
      predictionCutoff = cutoff
    )

    // Case C: no funding data recorded at all (the "-" placeholder case).
    val noFundingStartup = StartupRecord(
      permalink = "/organization/no-funding-example",
      name = "No Funding Example",
      homepageUrl = None, categoryList = Some("E-Commerce"),
      countryCode = Some("IND"), stateCode = None, region = None, city = None,
      fundingTotalUsd = None,
      fundingRounds = Some(1),
      foundedAt = LocalDate.of(2011, 1, 1),
      firstFundingAt = LocalDate.of(2011, 6, 1),
      lastFundingAt = Some(LocalDate.of(2011, 6, 1)),
      label = 0,
      predictionCutoff = cutoff
    )

    println("=== Case A: leakage-safe (lastFundingAt defined, pre-cutoff) ===")
    println(extractFinancialFeatures(safeStartup))
    println("Expect: totalFundingUsd=Some(5000000.0), fundingRoundCount=Some(3) - USED, not nulled\n")

    println("=== Case B: NOT leakage-safe (lastFundingAt = None) ===")
    println(extractFinancialFeatures(unsafeStartup))
    println("Expect: totalFundingUsd=None, fundingRoundCount=None DESPITE raw values existing -")
    println("        the guard must null them out to prevent leakage. fundingDataLeakageSafe=false.\n")

    println("=== Case C: no funding amount ever recorded ===")
    println(extractFinancialFeatures(noFundingStartup))
    println("Expect: hasFundingAmountRecorded=false, but fundingDataLeakageSafe=true")
    println("        (lastFundingAt IS defined here - the guard and the missing-amount flag are independent)")
  }
}
