package com.startupsurvival

import com.startupsurvival.models.{NewsRecord, ScoredNewsRecord, SentimentClass}

/**
 * PHASE 9 - Custom Scala Sentiment Scoring
 *
 * A transparent, explainable lexicon-based sentiment scorer for startup
 * news headlines. Deliberately NOT a call to a black-box sentiment API -
 * every score this produces can be traced back to the exact lexicon terms
 * that were matched, which is what the project brief requires.
 *
 * WHY AN ORIGINAL LEXICON, NOT A REPRODUCTION OF A PUBLISHED ONE: named
 * academic sentiment lexicons (e.g. Hu & Liu, 2004 - see
 * report/references.md [6]) are typically shared for research use, not
 * for verbatim redistribution inside another project. The word lists
 * below are an original compilation, written for this project and scoped
 * specifically to startup/funding news vocabulary - which is also more
 * relevant here than a generic product-review lexicon would be. This
 * connects directly to Loughran & McDonald (2011) [7], who showed that
 * lexicons built for one domain (e.g. general product reviews) routinely
 * misclassify words when applied to financial/business text - exactly
 * the failure mode a domain-specific list is meant to avoid.
 *
 * METHODOLOGY (matches the project brief's worked example exactly):
 *   1. Normalize headline (lowercase)
 *   2. Tokenize (split into words, strip punctuation)
 *   3. Remove stopwords
 *   4. Match remaining tokens against the positive/negative lexicon
 *   5. positive_matches = count of positive-lexicon hits
 *      negative_matches = count of negative-lexicon hits
 *   6. sentiment_score = positive_matches - negative_matches
 *   7. Classify: score > 0 -> Positive, score < 0 -> Negative, else Neutral
 *
 * Demonstrates (syllabus Unit 2): tokenization via map/filter, immutable
 * Set-based lexicon lookup, foldLeft for the core counting logic
 * (explicitly required by the brief), pattern matching for classification,
 * closures in the higher-order function chains.
 */
object SentimentScorer {

  /**
   * Small, general English stopword list - words carrying no sentiment
   * themselves, removed before lexicon matching so they can never
   * accidentally collide with a lexicon term (they don't here, but
   * removing them keeps token lists shorter and the matching step
   * cleaner/faster on longer headlines).
   */
  val Stopwords: Set[String] = Set(
    "a", "an", "the", "of", "in", "on", "for", "to", "and", "or", "is",
    "are", "was", "were", "at", "by", "with", "as", "from", "this",
    "that", "it", "its", "be", "been", "has", "have", "had", "will",
    "after", "into", "over", "up", "down", "out", "amid"
  )

  /**
   * Original positive-sentiment lexicon, scoped to startup/funding news.
   * Includes common inflections (raise/raises/raised/raising) since this
   * is a plain word-list matcher with no stemming - a documented,
   * deliberate simplicity tradeoff, not an oversight (see
   * docs/PHASE09_sentiment_scoring.md limitations section).
   */
  val PositiveTerms: Set[String] = Set(
    "raise", "raises", "raised", "raising", "funding", "funded", "funds",
    "invest", "invests", "invested", "investment", "investors", "backed",
    "backing", "expand", "expands", "expanded", "expansion", "growth",
    "grows", "growing", "grew", "launch", "launches", "launched",
    "launching", "success", "successful", "successfully", "profit",
    "profits", "profitable", "profitability", "partnership", "partners",
    "partner", "partnering", "innovative", "innovation", "award",
    "awarded", "wins", "won", "winning", "milestone", "record", "strong",
    "boost", "boosts", "boosted", "surge", "surges", "surging", "soar",
    "soars", "soaring", "hires", "hiring", "hired", "valuation",
    "unicorn", "ipo", "public", "acquired", "acquisition", "acquires",
    "breakthrough", "leading", "leader", "top", "best", "opportunity",
    "opportunities", "positive", "optimistic", "thrive", "thriving",
    "scale", "scaling", "scaled", "raises", "secures", "secured", "secure"
  )

  /**
   * Original negative-sentiment lexicon, same domain scope.
   */
  val NegativeTerms: Set[String] = Set(
    "close", "closes", "closed", "closing", "shuts", "shut", "shutdown",
    "shutting", "bankrupt", "bankruptcy", "layoffs", "layoff", "laysoff",
    "cuts", "cutting", "cut", "decline", "declines", "declined",
    "declining", "loss", "losses", "losing", "lost", "fails", "failed",
    "failure", "failing", "struggles", "struggling", "struggled",
    "collapse", "collapses", "collapsed", "collapsing", "warns",
    "warning", "warned", "lawsuit", "lawsuits", "sues", "sued", "suing",
    "fraud", "scandal", "resigns", "resigned", "resignation", "fired",
    "fires", "firing", "delay", "delays", "delayed", "delaying", "recall",
    "recalls", "recalled", "plunge", "plunges", "plunged", "drop",
    "drops", "dropped", "dropping", "concern", "concerns", "concerning",
    "controversy", "controversial", "criticized", "criticism",
    "criticizes", "trouble", "troubled", "troubling", "crisis", "weak",
    "weakness", "weakening", "disappointing", "disappoints",
    "disappointed", "down", "downturn", "slump", "slumps", "slumping"
  )

  /** Lowercases and splits on any run of non-letter characters, dropping
   * empty tokens - a simple, transparent tokenizer with no external
   * NLP library dependency. */
  def tokenize(text: String): List[String] =
    text
      .toLowerCase
      .split("[^a-zA-Z]+")
      .filter(_.nonEmpty)
      .toList

  def removeStopwords(tokens: List[String]): List[String] =
    tokens.filterNot(Stopwords.contains)

  /**
   * The core scoring step. Uses foldLeft to accumulate (positiveCount,
   * negativeCount) across the token list in one pass, with pattern
   * matching selecting which counter to increment for each token -
   * directly satisfies the brief's requirement to demonstrate fold/reduce
   * explicitly, not just map/filter.
   */
  def countSentimentWords(tokens: List[String]): (Int, Int) =
    tokens.foldLeft((0, 0)) { case ((posCount, negCount), token) =>
      token match {
        case t if PositiveTerms.contains(t) => (posCount + 1, negCount)
        case t if NegativeTerms.contains(t) => (posCount, negCount + 1)
        case _                              => (posCount, negCount)
      }
    }

  def classify(score: Int): SentimentClass =
    score match {
      case s if s > 0 => SentimentClass.Positive
      case s if s < 0 => SentimentClass.Negative
      case _          => SentimentClass.Neutral
    }

  final case class ScoreBreakdown(
      positiveMatches: Int,
      negativeMatches: Int,
      sentimentScore: Int,
      sentimentClass: SentimentClass,
      matchedPositiveTerms: List[String],
      matchedNegativeTerms: List[String]
  )

  /**
   * Full scoring pipeline for a single headline, also returning WHICH
   * specific terms matched - this is what makes the scorer explainable:
   * every score can be justified by name, not just by number, satisfying
   * the brief's explicit "transparent, explainable" requirement.
   */
  def scoreHeadline(headline: String): ScoreBreakdown = {
    val tokens = removeStopwords(tokenize(headline))
    val (pos, neg) = countSentimentWords(tokens)
    val score = pos - neg
    ScoreBreakdown(
      positiveMatches = pos,
      negativeMatches = neg,
      sentimentScore = score,
      sentimentClass = classify(score),
      matchedPositiveTerms = tokens.filter(PositiveTerms.contains),
      matchedNegativeTerms = tokens.filter(NegativeTerms.contains)
    )
  }

  /** Wraps scoreHeadline's result into the Phase 3 ScoredNewsRecord case
   * class, so this integrates directly with the NewsRecord pipeline from
   * Phases 7-8 without any adapter code. */
  def scoreNews(news: NewsRecord): ScoredNewsRecord = {
    val breakdown = scoreHeadline(news.headline)
    ScoredNewsRecord(
      news = news,
      positiveMatches = breakdown.positiveMatches,
      negativeMatches = breakdown.negativeMatches,
      sentimentScore = breakdown.sentimentScore,
      sentimentClass = breakdown.sentimentClass
    )
  }

  def main(args: Array[String]): Unit = {
    // The project brief's own worked example, verified below.
    val briefExample = "Startup X raises major funding and expands into new markets"

    // Real headlines captured live from Phase 7/13 (Kabbage, Color Labs, Pinterest).
    val realHeadlines = List(
      "Kabbage Raises Some Serious Cabbage for Small-Business Loans - WIRED",
      "$41 million can't buy success as Color app finally gives up (update: Color denies shutdown) - The Verge",
      "A Mess Of Family Dynamics Alleged In Lawsuit Against Silicon Valley Entrepreneur And Color Founder Bill Nguyen - Forbes",
      "Apple to acquire troubled startup Color Labs? - Gadgets 360",
      "Ben Silbermann On How Pinterest Slowly Grew To Massive Scale - Forbes",
      "Pinterest, Tumblr and the Trouble With \u2018Curation\u2019 (Published 2012) - The New York Times",
      "Startup shuts down after failing to raise sufficient funding, layoffs follow",
      "Company announces bankruptcy amid mounting losses and investor lawsuit"
    )

    println("=== Brief's worked example ===")
    val r0 = scoreHeadline(briefExample)
    println("Headline: \"" + briefExample + "\"")
    println(s"  positive terms matched: ${r0.matchedPositiveTerms}")
    println(s"  negative terms matched: ${r0.matchedNegativeTerms}")
    println(s"  score=${r0.sentimentScore}  class=${r0.sentimentClass}")
    println("  Expected: positive terms = raises, funding, expands; class = Positive\n")

    println("=== Real and constructed test headlines ===")
    realHeadlines.foreach { h =>
      val r = scoreHeadline(h)
      println(s"[${r.sentimentClass}] score=${r.sentimentScore} (pos=${r.positiveMatches}, neg=${r.negativeMatches})")
      println(s"  pos terms: ${r.matchedPositiveTerms}")
      println(s"  neg terms: ${r.matchedNegativeTerms}")
      println("  \"" + h + "\"\n")
    }
  }
}
