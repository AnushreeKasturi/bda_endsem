package com.startupsurvival

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Try
import com.startupsurvival.models.StartupRecord

object DataCleaner {

  private val DateFormatter = DateTimeFormatter.ISO_LOCAL_DATE

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

  def loadStartups(path: String): List[StartupRecord] = {
    val source = Source.fromFile(path)
    try {
      val lines = source.getLines().toList
      val dataLines = if (lines.nonEmpty) lines.tail else Nil
      dataLines
        .map(parseCsvLine)
        .flatMap(rowToStartupRecord)
    } finally {
      source.close()
    }
  }

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
