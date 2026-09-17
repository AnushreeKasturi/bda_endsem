package com.startupsurvival

import java.net.{URI, URLEncoder}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import java.util.Locale
import scala.io.Source
import scala.util.Try

/**
 * PHASE 7 - News Collection
 *
 * Fetches candidate news headlines for a startup from Google News RSS.
 *
 * WHY GOOGLE NEWS RSS AND NOT NEWSAPI/GNEWS:
 * Verified in Phase 1 (docs/PHASE01_dataset_analysis.md S1.4): NewsAPI's
 * free tier only retains ~1 month of history and forbids production use;
 * GNews's free tier only reaches back to 2020. Neither can reach our
 * 2013-01-01 cutoff. Google News RSS is free, has no documented request
 * quota for this scale of use, and is the only realistic option - but its
 * ARCHIVAL DEPTH for a specific small startup is not guaranteed and must
 * be checked per-company, which is exactly what the feasibility spike
 * (see docs/PHASE07_news_collection.md) exists to establish honestly.
 *
 * WHY NO XML LIBRARY DEPENDENCY: rather than add scala-xml as a build.sbt
 * dependency for a very small, fixed RSS shape, this file extracts <item>
 * blocks and their <title>/<pubDate>/<link> children directly with
 * targeted regexes. This keeps the build simple (no new dependency to
 * resolve) and is defensible for RSS specifically, which is a much more
 * constrained format than general HTML/XML - but note this is NOT a
 * general-purpose XML parser and would need to be replaced with a real
 * one (scala-xml, or Java's javax.xml) if the input format were less
 * predictable than Google's own RSS output.
 *
 * NOT TESTED AGAINST A LIVE NETWORK CALL FROM THIS DEVELOPMENT ENVIRONMENT
 * - the sandbox used to write and verify this code cannot reach
 * news.google.com. The parsing logic (parseRssItems) IS verified against
 * a captured sample RSS payload (see the ...Test object at the bottom of
 * this file / docs/PHASE07_news_collection.md). The live fetch
 * (fetchUrl + the end-to-end flow) must be verified by running this on a
 * machine with normal internet access - that verification step is your
 * next action after reviewing this file.
 */
object NewsCollector {

  private val PredictionCutoff = LocalDate.of(2013, 1, 1)

  /** How far back from the cutoff to request news for - see Phase 2 R4:
   * only news strictly before the cutoff is feature-eligible. */
  private val LookbackMonths = 24

  final case class RawNewsItem(
      title: String,
      link: String,
      pubDateRaw: String,
      source: Option[String]
  )

  /**
   * Builds a Google News RSS search URL for a company name. Uses Google's
   * `before:`/`after:` search operators to bias results toward the
   * pre-cutoff window - this is a best-effort request, not a guarantee;
   * the feasibility spike must confirm whether Google actually honors
   * these operators for News results as reliably as it does for plain
   * web search.
   */
  def buildRssUrl(companyName: String, cutoff: LocalDate = PredictionCutoff): String = {
    val windowStart = cutoff.minusMonths(LookbackMonths)
    val query = s""""$companyName" before:${cutoff} after:${windowStart}"""
    val encoded = URLEncoder.encode(query, "UTF-8")
    s"https://news.google.com/rss/search?q=$encoded&hl=en-US&gl=US&ceid=US:en"
  }

  /**
   * Fetches raw text from a URL with a browser-like User-Agent (Google
   * has been known to reject Java's default UA string) and a timeout, so
   * a hung request cannot stall a batch collection run indefinitely.
   */
  def fetchUrl(url: String, timeoutMs: Int = 10000): Try[String] = Try {
    val connection = new URI(url).toURL.openConnection()
    connection.setConnectTimeout(timeoutMs)
    connection.setReadTimeout(timeoutMs)
    connection.setRequestProperty(
      "User-Agent",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    val source = Source.fromInputStream(connection.getInputStream, "UTF-8")
    try source.mkString
    finally source.close()
  }

  /**
   * Extracts <item>...</item> blocks from an RSS payload and pulls out
   * title/link/pubDate/source from each. Uses non-greedy regex matching
   * scoped to one <item> at a time (never matching across item
   * boundaries), and strips Google's CDATA wrapping where present.
   *
   * Demonstrates: pattern matching via regex extraction, Option handling
   * for fields that may be absent, immutable List construction via map.
   */
  def parseRssItems(rssXml: String): List[RawNewsItem] = {
    val itemPattern = "(?s)<item>(.*?)</item>".r
    val titlePattern = "(?s)<title>(.*?)</title>".r
    val linkPattern = "(?s)<link>(.*?)</link>".r
    val pubDatePattern = "(?s)<pubDate>(.*?)</pubDate>".r
    val sourcePattern = "(?s)<source[^>]*>(.*?)</source>".r

    def stripCdata(s: String): String =
      s.trim
        .stripPrefix("<![CDATA[")
        .stripSuffix("]]>")
        .trim

    def firstMatch(pattern: scala.util.matching.Regex, in: String): Option[String] =
      pattern.findFirstMatchIn(in).map(m => stripCdata(m.group(1)))

    itemPattern
      .findAllMatchIn(rssXml)
      .map(_.group(1))
      .flatMap { itemBody =>
        for {
          title <- firstMatch(titlePattern, itemBody)
          link  <- firstMatch(linkPattern, itemBody)
        } yield RawNewsItem(
          title = title,
          link = link,
          pubDateRaw = firstMatch(pubDatePattern, itemBody).getOrElse(""),
          source = firstMatch(sourcePattern, itemBody)
        )
      }
      .toList
  }

  /** RSS pubDate is RFC-822 format, e.g. "Wed, 12 Jan 2011 05:00:00 GMT". */
  private val RssDateFormatter = DateTimeFormatter
    .ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.ENGLISH)

  def parsePubDate(raw: String): Option[LocalDate] =
    Try {
      val parsed = RssDateFormatter.parse(raw.trim)
      LocalDate.of(
        parsed.get(ChronoField.YEAR),
        parsed.get(ChronoField.MONTH_OF_YEAR),
        parsed.get(ChronoField.DAY_OF_MONTH)
      )
    }.toOption

  /**
   * End-to-end: fetch + parse + filter to only pre-cutoff items (Phase 2
   * R4 enforced again here, at collection time - never rely on an
   * upstream filter alone, matching the same defensive pattern used in
   * FeatureExtractor's SentimentFeatures aggregation, Phase 10).
   */
  def collectFor(
      companyName: String,
      cutoff: LocalDate = PredictionCutoff
  ): List[RawNewsItem] = {
    val url = buildRssUrl(companyName, cutoff)
    fetchUrl(url) match {
      case scala.util.Success(xml) =>
        parseRssItems(xml).filter { item =>
          parsePubDate(item.pubDateRaw).exists(_.isBefore(cutoff))
        }
      case scala.util.Failure(e) =>
        println(s"  [WARN] fetch failed for '$companyName': ${e.getMessage}")
        Nil
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) {
      println("Usage: NewsCollector \"Company Name\" [\"Another Company\" ...]")
      println("This is a MANUAL FEASIBILITY SPIKE, not a batch collector.")
      println("Run against a handful of companies first - do not point this")
      println("at all 22,075 startups without reviewing docs/PHASE07_news_collection.md.")
      sys.exit(1)
    }
    args.foreach { name =>
      println(s"\n=== $name ===")
      println(s"URL: ${buildRssUrl(name)}")
      val items = collectFor(name)
      println(s"Pre-cutoff items found: ${items.length}")
      items.take(5).foreach { item =>
        println(s"  - [${item.pubDateRaw}] ${item.title}")
      }
    }
  }
}
