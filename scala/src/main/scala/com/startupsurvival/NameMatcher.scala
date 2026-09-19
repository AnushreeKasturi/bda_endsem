package com.startupsurvival

import scala.util.matching.Regex

/**
 * PHASE 8 - News Cleaning and Startup-Name Matching
 *
 * Given a company name and a candidate news headline (from Phase 7's
 * NewsCollector), decides whether the headline is actually about that
 * company and with what confidence - producing the news_match_confidence
 * field the project brief requires.
 *
 * WHY THIS IS NEEDED, WITH A REAL EXAMPLE: Phase 7's live test run
 * against Google News RSS returned a genuine false positive for
 * "Kabbage" - an article titled "The tomato barons of the occupied
 * Western Sahara" that never mentions Kabbage anywhere in its title.
 * Google's own relevance ranking let this through; this file's job is to
 * catch cases like it BEFORE they become a sentiment feature, by
 * requiring the company name (or a majority of its tokens) to actually
 * appear in the headline text, not just trusting the upstream search
 * result. That exact article is used as a live verification case below.
 *
 * Demonstrates (syllabus Unit 1/2): pattern matching, higher-order
 * functions (map/filter/count), immutable case classes, regex as a
 * first-class value, closures.
 */
object NameMatcher {

  /** Company-name suffixes stripped before matching, so "Kabbage Inc"
   * and "Kabbage" are treated identically. */
  private val CorporateSuffixes: Set[String] =
    Set("inc", "llc", "ltd", "corp", "corporation", "co", "company",
        "group", "holdings", "plc", "gmbh")

  /**
   * Ambiguity threshold reused directly from Phase 1's inspection script
   * (scripts/inspect_dataset.py S10): a single-word name of 4 characters
   * or fewer was flagged there as "very high risk" (1,889 / 66,368 rows,
   * 2.8%). Using the same threshold here keeps Phase 1's finding and
   * Phase 8's matching behavior consistent rather than inventing a new,
   * disconnected definition of "ambiguous."
   */
  def isAmbiguousName(originalName: String): Boolean = {
    val tokens = originalName.trim.split("\\s+").filter(_.nonEmpty)
    tokens.length == 1 && originalName.trim.length <= 4
  }

  /**
   * Lowercases, strips punctuation, collapses whitespace, and removes
   * known corporate suffixes. Applied identically to both the company
   * name and the headline text before comparison, so matching is
   * case/punctuation-insensitive by construction.
   */
  def normalize(text: String): String = {
    val lower = text.toLowerCase
    val noPunct = lower.replaceAll("[^a-z0-9\\s]", " ")
    val collapsed = noPunct.trim.replaceAll("\\s+", " ")
    val tokens = collapsed.split(" ").filter(_.nonEmpty).toList
    val stripped = tokens.filterNot(CorporateSuffixes.contains)
    stripped.mkString(" ")
  }

  /** Word-boundary containment check - "kabbage" must appear as a whole
   * token in the haystack, not as a substring inside an unrelated word
   * (e.g. must not match inside "cabbagepatch" or similar). */
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

  /**
   * Core scoring function. Three tiers:
   *   1. Ambiguous short name (Phase 1's threshold) - capped confidence
   *      even on a literal match, per the project brief's instruction to
   *      "mark it as unreliable rather than forcing a match."
   *   2. Full normalized name appears as a contiguous phrase in the
   *      headline - highest confidence (1.0).
   *   3. Multi-word name: at least half its tokens appear (not
   *      necessarily contiguous) - partial, discounted confidence.
   *   4. Otherwise - rejected (confidence 0.0, matched = false). This is
   *      the branch that correctly rejects the "tomato barons" article.
   */
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

  /**
   * Filters a list of (headline, ...) candidates down to accepted
   * matches only, pairing each with its MatchResult. Demonstrates
   * filter+map used together rather than a manual loop with an
   * if-then-append pattern.
   */
  def filterMatches[A](
      companyName: String,
      candidates: List[A]
  )(headlineOf: A => String): List[(A, MatchResult)] =
    candidates
      .map(item => (item, computeMatchConfidence(companyName, headlineOf(item))))
      .filter { case (_, result) => result.matched }

  def main(args: Array[String]): Unit = {
    // Live verification data, captured directly from Phase 7's actual
    // Google News RSS run - not synthetic. See docs/PHASE07_news_collection.md.
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
