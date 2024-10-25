package ch.epfl.lts2.wikipedia

import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.util.Date
import java.time.LocalDateTime
import org.apache.spark.sql.{Encoder, Encoders}

// Enumeration for dump types
object WikipediaDumpType extends Enumeration {
  val Page = Value("page")
  val PageLinks = Value("pagelinks")
  val Redirect = Value("redirect")
  val Category = Value("category")
  val CategoryLinks = Value("categorylinks")
  val LangLinks = Value("langlinks")
  val LinkTarget = Value("linktarget")
  val JoinedPageLink = Value("joinedpagelink")
}

// Enumeration for namespaces
object WikipediaNamespace extends Enumeration {
  val Dummy: Int = -1
  val Page: Int = 0
  val Category: Int = 14
  val Portal: Int = 100
  val Book: Int = 108
}

case class DumpInfo(langCode: String, dateCode: String, dumpType: String)

abstract class WikipediaElement extends Serializable

case class WikipediaPage(id: Int, namespace: Int, title: String,
                         isRedirect: Boolean, isNew: Boolean, random: Double, touched: Timestamp,
                         linksUpdated: Timestamp, latest: Int, len: Int,
                         contentModel: String, lang: String) extends WikipediaElement

object WikipediaPage {
  implicit val encoder: Encoder[WikipediaPage] = Encoders.product[WikipediaPage]
}


case class WikipediaPageLink(from: Int, fromNamespace: Int, targetId: Long) extends WikipediaElement

object WikipediaPageLink {
  implicit val encoder: Encoder[WikipediaPageLink] = Encoders.product[WikipediaPageLink]
}

case class WikipediaSimplePage(id: Int, title: String, isRedirect: Boolean, isNew: Boolean) extends WikipediaElement

object WikipediaSimplePage {
  implicit val encoder: Encoder[WikipediaSimplePage] = Encoders.product[WikipediaSimplePage]
}

case class WikipediaLangLink(from: Int, lang: String, title: String) extends WikipediaElement

object WikipediaLangLink {
  implicit val encoder: Encoder[WikipediaLangLink] = Encoders.product[WikipediaLangLink]
}

case class WikipediaRedirect(from: Int, targetNamespace: Int, title: String, interwiki: Option[String], fragment: Option[String]) extends WikipediaElement

object WikipediaRedirect {
  implicit val encoder: Encoder[WikipediaRedirect] = Encoders.product[WikipediaRedirect]
}

case class WikipediaCategory(id: Int, title: String, pages: Int, subcats: Int, files: Int) extends WikipediaElement

object WikipediaCategory {
  implicit val encoder: Encoder[WikipediaCategory] = Encoders.product[WikipediaCategory]
}

case class WikipediaCategoryLink(from: Int, to: String, sortKey: String, timestamp: Timestamp,
                                 sortkeyPrefix: String, collation: String, ctype: String) extends WikipediaElement

object WikipediaCategoryLink {
  implicit val encoder: Encoder[WikipediaCategoryLink] = Encoders.product[WikipediaCategoryLink]
}

case class MergedPageLink(from: Int, id: Int, title: String, fromNamespace: Int, namespace: Int) extends WikipediaElement

object MergedPageLink {
  implicit val encoder: Encoder[MergedPageLink] = Encoders.product[MergedPageLink]
}

case class MergedRedirect(from: Int, id: Int, title: String) extends WikipediaElement

object MergedRedirect {
  implicit val encoder: Encoder[MergedRedirect] = Encoders.product[MergedRedirect]
}

case class MergedCatlink(from: Int, id: Int, title: String, ctype: String) extends WikipediaElement

object MergedCatlink {
  implicit val encoder: Encoder[MergedCatlink] = Encoders.product[MergedCatlink]
}

case class WikipediaPageLang(id: Int, namespace: Int, title: String,
                             isRedirect: Boolean, isNew: Boolean, random: Double, touched: Timestamp,
                             latest: Int, len: Int, contentModel: String, languageCode: String) extends WikipediaElement

object WikipediaPageLang {
  implicit val encoder: Encoder[WikipediaPageLang] = Encoders.product[WikipediaPageLang]
}

case class WikipediaPagecount(languageCode: String, title: String, namespace: Int, source: String, dailyVisits: Int, hourlyVisits: String) extends WikipediaElement

object WikipediaPagecount {
  implicit val encoder: Encoder[WikipediaPagecount] = Encoders.product[WikipediaPagecount]
}

case class WikipediaHourlyVisit(time: LocalDateTime, visits: Int) extends WikipediaElement

object WikipediaHourlyVisit {
  implicit val encoder: Encoder[WikipediaHourlyVisit] = Encoders.product[WikipediaHourlyVisit]
}

// New LinkTarget case class and encoder
case class LinkTarget(lt_id: Int, lt_namespace: Int, lt_title: String) extends WikipediaElement

object LinkTarget {
  implicit val encoder: Encoder[LinkTarget] = Encoders.product[LinkTarget]
}

case class JoinedPageLink(from: Int, fromNamespace: Int, targetId: Long, title: String, namespace: Int) extends WikipediaElement

object JoinedPageLink {
  implicit val encoder: Encoder[JoinedPageLink] = Encoders.product[JoinedPageLink]
}

