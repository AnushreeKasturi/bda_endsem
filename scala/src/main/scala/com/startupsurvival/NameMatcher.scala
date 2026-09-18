package com.startupsurvival

import scala.util.matching.Regex

object NameMatcher {

  private val CorporateSuffixes: Set[String] =
    Set("inc", "llc", "ltd", "corp", "corporation", "co", "company",
        "group", "holdings", "plc", "gmbh")

  def isAmbiguousName(originalName: String): Boolean = {
    val tokens = originalName.trim.split("\\s+").filter(_.nonEmpty)
    tokens.length == 1 && originalName.trim.length <= 4
  }

  def normalize(text: String): String = {
    val lower = text.toLowerCase
    val noPunct = lower.replaceAll("[^a-z0-9\\s]", " ")
    val collapsed = noPunct.trim.replaceAll("\\s+", " ")
    val tokens = collapsed.split(" ").filter(_.nonEmpty).toList
    val stripped = tokens.filterNot(CorporateSuffixes.contains)
    stripped.mkString(" ")
  }

  private def containsWholeWord(haystack: String, needle: String): Boolean = {
    if (needle.isEmpty) false
    else {
      val pattern = new Regex("\\b" + Regex.quote(needle) + "\\b")
      pattern.findFirstIn(haystack).isDefined
    }
  }

  final case class MatchResult(
      matched: Boolean,
      confidence: Double,
      reason: String
  )

  def computeMatchConfidence(companyName: String, headline: String): MatchResult = {
    val normName = normalize(companyName)
    val normHeadline = normalize(headline)
    val nameTokens = normName.split(" ").filter(_.nonEmpty).toList

    if (nameTokens.isEmpty) {
      MatchResult(matched = false, confidence = 0.0, reason = "empty normalized name")
    } else if (isAmbiguousName(companyName)) {
      val exact = containsWholeWord(normHeadline, normName)
      if (exact)
        MatchResult(matched = true, confidence = 0.3,
          reason = "ambiguous short name (<=4 chars, single word) - capped confidence")
      else
        MatchResult(matched = false, confidence = 0.0,
          reason = "ambiguous short name - no match found")
    } else {
      val exactPhrase = containsWholeWord(normHeadline, normName)
      if (exactPhrase) {
        MatchResult(matched = true, confidence = 1.0, reason = "exact phrase match")
      } else {
        val matchedCount = nameTokens.count(t => containsWholeWord(normHeadline, t))
        val ratio = matchedCount.toDouble / nameTokens.length
        if (ratio >= 0.5)
          MatchResult(
            matched = true,
            confidence = 0.5 + 0.3 * ratio,
            reason = s"partial token match ($matchedCount/${nameTokens.length} tokens)"
          )
        else
          MatchResult(matched = false, confidence = 0.0,
            reason = s"insufficient token overlap ($matchedCount/${nameTokens.length} tokens)")
      }
    }
  }

  def filterMatches[A](
      companyName: String,
      candidates: List[A]
  )(headlineOf: A => String): List[(A, MatchResult)] =
    candidates
      .map(item => (item, computeMatchConfidence(companyName, headlineOf(item))))
      .filter { case (_, result) => result.matched }

  def main(args: Array[String]): Unit = {
    val kabbageHeadlines = List(
      "Kabbage Raises Some Serious Cabbage for Small-Business Loans - WIRED",
      "Kabbage: The Merchant Cash Advance of the Online Business World - deBanked",
      "The tomato barons of the occupied Western Sahara - Western Sahara Resource Watch",
      "Microlender helps e-tailer keep the orders moving | Business - Las Vegas Review-Journal",
      "Funding roundup - week ending 08/19/11 - vator.tv"
    )

    println("=== NameMatcher verification against live Phase 7 Kabbage results ===\n")
    kabbageHeadlines.foreach { headline =>
      val result = computeMatchConfidence("Kabbage", headline)
      val verdict = if (result.matched) "ACCEPT" else "REJECT"
      println(s"[$verdict] confidence=${result.confidence}  ${result.reason}")
      println("         \"" + headline + "\"")
      println("")
    }

    val (accepted, _) = kabbageHeadlines
      .map(h => (h, computeMatchConfidence("Kabbage", h)))
      .partition { case (_, r) => r.matched }
    println(s"Accepted ${accepted.length} of ${kabbageHeadlines.length} headlines.")
    println("Expect the tomato-barons article rejected (it must be); expect some")
    println("genuinely relevant roundup-style articles rejected too, since their")
    println("titles don't contain the company name even though their body text")
    println("likely does - see docs/PHASE08_name_matching.md for why this")
    println("precision-over-recall tradeoff is a deliberate, documented choice.")
  }
}
