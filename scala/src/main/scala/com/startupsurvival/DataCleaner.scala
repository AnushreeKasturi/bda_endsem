package com.startupsurvival

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Try
import com.startupsurvival.models.StartupRecord

/**
 * PHASE 6 - Startup Data Preprocessing
 *
 * Reads Phase 2's canonical output (data/processed/labeled_eligible_startups.csv)
 * and parses it into immutable StartupRecord instances (Phase 3).
 *
 * This is deliberately plain Scala, not Spark - Spark's RDD pipeline is
 * Phase 12. The point of this phase is to demonstrate the syllabus Unit 2
 * functional-programming toolkit directly on real project data:
 *   - immutable case classes and collections (List, never mutated)
 *   - higher-order functions: map, flatMap, filter, count, partition, foldLeft
 *   - pattern matching: on CSV field lists, on quote-state while parsing,
 *     on missing-value sentinels
 *   - Option[_] for principled null-handling instead of null/exceptions
 *   - closures (used implicitly throughout the map/filter chains below)
 *
 * IMPORTANT - why this file has its own CSV line parser rather than
 * splitting on comma: Phase 4 found a real bug where a naive
 * comma-split silently corrupted data whenever a field (e.g. a company
 * name) contained a comma inside quotes. That bug lived in Hive's table
 * definition, not in Python or Spark, precisely because Hive's default
 * reader does not respect quoting. This file must not repeat that
 * mistake, so parseCsvLine below is a small quote-aware state machine,
 * not a String.split(",").
 */
object DataCleaner {

  private val DateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

  /**
   * Parses one CSV line into its fields, respecting double-quoted fields
   * that may contain embedded commas (matching pandas' to_csv() quoting,
   * which is what produced this file in Phase 2).
   *
   * Implemented as a foldLeft over the characters of the line, carrying an
   * immutable ParseState forward - no mutable var, no index-based loop.
   * Demonstrates: foldLeft (a core Unit 2 higher-order function), pattern
   * matching on (character, inQuotes) pairs, and immutability (each step
   * produces a brand new ParseState rather than mutating one in place).
   */
  private case class ParseState(
      completedFields: List[String],
      current: String,
      inQuotes: Boolean
  )

  def parseCsvLine(line: String): List[String] = {
    val finalState = line.foldLeft(ParseState(Nil, "", inQuotes = false)) {
      (state, ch) =>
        (ch, state.inQuotes) match {
          case ('"', _) =>
            state.copy(inQuotes = !state.inQuotes)
          case (',', false) =>
            state.copy(
              completedFields = state.completedFields :+ state.current,
              current = ""
            )
          case (c, _) =>
            state.copy(current = state.current + c)
        }
    }
    finalState.completedFields :+ finalState.current
  }

  /**
   * funding_total_usd carries the raw "-" placeholder for missing values
   * (Phase 1 found this affects ~19.3% of rows). Pattern matching turns
   * that sentinel into a proper None rather than a parse exception or a
   * silently-wrong zero.
   */
  def parseFundingUsd(raw: String): Option[Double] = raw.trim match {
    case "" | "-" => None
    case s        => Try(s.toDouble).toOption
  }

  def parseOptionalString(raw: String): Option[String] = raw.trim match {
    case "" | "NULL" | "null" => None
    case s                    => Some(s)
  }

  def parseOptionalInt(raw: String): Option[Int] =
    Try(raw.trim.toInt).toOption

  def parseRequiredDate(raw: String): Option[LocalDate] =
    Try(LocalDate.parse(raw.trim, DateFormatter)).toOption

  def parseOptionalDate(raw: String): Option[LocalDate] = raw.trim match {
    case "" | "NULL" | "null" => None
    case s                    => Try(LocalDate.parse(s, DateFormatter)).toOption
  }

  /**
   * Converts one parsed field list into a StartupRecord, using a
   * for-comprehension over Options so that any required field failing to
   * parse (founded_at, first_funding_at, prediction_cutoff, label) causes
   * the whole row to become None rather than a partially-built record.
   *
   * The pattern match on the field-count shape (exactly 15 columns, `Nil`
   * at the tail) protects against silently misparsed rows - if the column
   * count is ever wrong, this row is dropped and counted as unparsed
   * rather than read into the wrong fields.
   */
  def rowToStartupRecord(fields: List[String]): Option[StartupRecord] =
    fields match {
      case permalink :: name :: homepageUrl :: categoryList :: fundingTotalUsd ::
          countryCode :: stateCode :: region :: city :: fundingRounds ::
          foundedAt :: firstFundingAt :: lastFundingAt :: label ::
          predictionCutoff :: Nil =>
        for {
          founded      <- parseRequiredDate(foundedAt)
          firstFunding <- parseRequiredDate(firstFundingAt)
          cutoff       <- parseRequiredDate(predictionCutoff)
          lbl          <- Try(label.trim.toInt).toOption
        } yield StartupRecord(
          permalink = permalink,
          name = name,
          homepageUrl = parseOptionalString(homepageUrl),
          categoryList = parseOptionalString(categoryList),
          countryCode = parseOptionalString(countryCode),
          stateCode = parseOptionalString(stateCode),
          region = parseOptionalString(region),
          city = parseOptionalString(city),
          fundingTotalUsd = parseFundingUsd(fundingTotalUsd),
          fundingRounds = parseOptionalInt(fundingRounds),
          foundedAt = founded,
          firstFundingAt = firstFunding,
          lastFundingAt = parseOptionalDate(lastFundingAt),
          label = lbl,
          predictionCutoff = cutoff
        )
      case _ =>
        None
    }

  /**
   * Loads the full file into an immutable List[StartupRecord].
   *
   * flatMap here does two jobs at once: it applies rowToStartupRecord to
   * every line AND drops every None (a malformed row) in a single pass -
   * a direct, idiomatic use of a Unit 2 higher-order function rather than
   * a manual loop with an if-check and a mutable buffer.
   */
  def loadStartups(path: String): List[StartupRecord] = {
    val source = Source.fromFile(path)
    try {
      val lines = source.getLines().toList
      val dataLines = if (lines.nonEmpty) lines.tail else Nil // skip header
      dataLines
        .map(parseCsvLine)
        .flatMap(rowToStartupRecord)
    } finally {
      source.close()
    }
  }

  /**
   * Summary statistics computed entirely with higher-order functions:
   * partition (splits into two immutable lists by a predicate - a direct
   * syllabus-named Unit 2 concept), count, flatMap (again dropping the
   * Nones from fundingTotalUsd), and sum/length in place of a manual fold
   * for the average.
   */
  def summarize(records: List[StartupRecord]): Unit = {
    val total = records.length
    val (closed, operating) = records.partition(_.label == 0)
    val missingFunding = records.count(_.fundingTotalUsd.isEmpty)
    val knownFunding = records.flatMap(_.fundingTotalUsd)
    val avgFunding =
      if (knownFunding.isEmpty) 0.0 else knownFunding.sum / knownFunding.length

    println(s"Loaded $total startup records")
    println(s"  closed    (label 0): ${closed.length}")
    println(s"  operating (label 1): ${operating.length}")
    println(
      f"  missing funding_total_usd: $missingFunding (${missingFunding.toDouble / total * 100}%.1f%%)"
    )
    println(f"  average funding (where known): $$${avgFunding}%,.0f")
  }

  def main(args: Array[String]): Unit = {
    val path =
      if (args.nonEmpty) args(0)
      else "../data/processed/labeled_eligible_startups.csv"
    println(s"Loading startups from: $path")
    val records = loadStartups(path)
    summarize(records)
  }
}
