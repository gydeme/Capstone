package ch.epfl.lts2.wikipedia

import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.time._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.log4j.Logger

trait ElementFilter[T <: WikipediaElement with Product] extends Serializable {
  def filterElt(t: T): Boolean
}

sealed class DefaultElementFilter[T <: WikipediaElement with Product] extends ElementFilter[T] {
  def filterElt(t: T): Boolean = true
}

trait WikipediaElementParserBase[T <: WikipediaElement with Product] {
  def parseLine(lineInput: String): List[T]
  def getRDD(lines: RDD[String]): RDD[T]
  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame
  def defaultFilterElt(t: T): Boolean
  def filterElt(t: T): Boolean
  def logSampleData(rdd: RDD[String], name: String): Unit
}

abstract class WikipediaElementParser[T <: WikipediaElement with Product](elementFilter: ElementFilter[T])
  extends WikipediaElementParserBase[T] with Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)
  override def filterElt(t: T): Boolean = elementFilter.filterElt(t) && defaultFilterElt(t)

  def logSampleData(rdd: RDD[String], name: String): Unit = {
//    val sampleData = rdd.take(10).mkString("\n")
//    logger.info(s"Sample data for $name:\n$sampleData")
    //commented out for performance
  }
}

class WikipediaPageParser(elementFilter: ElementFilter[WikipediaPage] = new DefaultElementFilter[WikipediaPage])
  extends WikipediaElementParser[WikipediaPage](elementFilter) {
  //2024 1.43 dump:
  // +--------------------+---------------------+------+-----+---------+----------------+
  // | Field              | Type                | Null | Key | Default | Extra          |
  // +--------------------+---------------------+------+-----+---------+----------------+
  // | page_id            | int(10) unsigned    | NO   | PRI | NULL    | (\d+)          |
  // | page_namespace     | int(11)             | NO   | MUL | NULL    | (\d+)          |
  // | page_title         | varbinary(255)      | NO   |     | NULL    | '(.*?)'        |
  // | page_is_redirect   | tinyint(3) unsigned | NO   | MUL | 0       | ([01])         |
  // | page_is_new        | tinyint(3) unsigned | NO   |     | 0       | ([01])         |
  // | page_random        | double unsigned     | NO   | MUL | NULL    | ([\d\.]+       |
  // | page_touched       | binary(14)          | NO   |     | NULL    | '(\d{14})'     |
  // | page_links_updated | binary(14)          | YES  |     | NULL    |(NULL|'(\d{14})')|
  // | page_latest        | int(10) unsigned    | NO   |     | NULL    | (\d+)          |
  // | page_len           | int(10) unsigned    | NO   | MUL | NULL    | (\d+)          |
  // | page_content_model | varbinary(32)       | YES  |     | NULL    | (NULL|'[^']*') |
  // | page_lang          | varbinary(35)       | YES  |     | NULL    | (NULL|'[^']*') |
  //  +--------------------+---------------------+------+-----+---------+----------------+
  // Updated regex to match the new 2024 schema
  // val pageRegex = """\((\d+),(\d+),'([^#<>{}|%]+)',([01]),([01]),([\d\.]+),'(\d{14})',(NULL|'(\d{14})'),(\d+),(\d+),(NULL|'[^']*'),(NULL|'[^']*')\)""".r
  val pageRegex = """\((\d+),(\d+),'(.*?)',([01]),([01]),([\d.]+),'(\d{14})','(\d{14})',(\d+),(\d+),(NULL|'[^']*'),(NULL|'[^']*')\)""".r



  val timestampFormat = new SimpleDateFormat("yyyyMMddHHmmss")

//  def parseLine(lineInput: String): List[WikipediaPage] = {
//    val r = pageRegex.findAllIn(lineInput).matchData.toList
//    if (r.isEmpty) {
//      println(s"No match for line: $lineInput")
//    }
//    r.map(m => WikipediaPage(m.group(1).toInt, m.group(2).toInt, m.group(3),
//      m.group(4).toInt == 1, m.group(5).toInt == 1, m.group(6).toDouble,
//      new Timestamp(timestampFormat.parse(m.group(7)).getTime),
//      m.group(8).toInt, m.group(9).toInt, m.group(10)))
//  }

  def parseLine(lineInput: String): List[WikipediaPage] = {
    val r = pageRegex.findAllIn(lineInput).matchData.toList
    r.map { m =>
      WikipediaPage(
        m.group(1).toInt,   // page_id
        m.group(2).toInt,   // page_namespace
        m.group(3),         // page_title
        m.group(4).toInt == 1,  // page_is_redirect
        m.group(5).toInt == 1,  // page_is_new
        m.group(6).toDouble,    // page_random
        // Parse page_touched as a timestamp, removing any quotes
        new Timestamp(timestampFormat.parse(m.group(7)).getTime),  // page_touched
        // Parse page_links_updated as a timestamp (handle NULL cases)
        if (m.group(8) == "NULL") null else new Timestamp(timestampFormat.parse(m.group(8)).getTime),  // page_links_updated
        m.group(9).toInt,   // page_latest
        m.group(10).toInt,  // page_len
        // Handling NULL for page_content_model
        if (m.group(11) == "NULL") "" else m.group(11),   // page_content_model: default to empty string if NULL
        // Handling NULL for page_lang
        if (m.group(12) == "NULL") null else m.group(12)  // page_lang: optional field
      )
    }
  }




  def defaultFilterElt(t: WikipediaPage): Boolean =
    t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category

  def getRDD(lines: RDD[String]): RDD[WikipediaPage] = {
    logSampleData(lines, "WikipediaPage")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaPage Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaPageLinkParser(elementFilter: ElementFilter[WikipediaPageLink] = new DefaultElementFilter[WikipediaPageLink])
  extends WikipediaElementParser[WikipediaPageLink](elementFilter) {

  // Updated regex for 2024 schema to handle pl_target_id
  val plRegex = """\((\d+),(\d+),(\d+)\)""".r

  def parseLine(lineInput: String): List[WikipediaPageLink] = {
    val r = plRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaPageLink(m.group(1).toInt, m.group(2).toInt, m.group(3).toLong))
  }


//  def defaultFilterElt(t: WikipediaPageLink): Boolean = {
//    (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category) &&
//      (t.fromNamespace == WikipediaNamespace.Page || t.fromNamespace == WikipediaNamespace.Category)
//  }
  def defaultFilterElt(t: WikipediaPageLink): Boolean = true //dummy function now that pagelink has no namespace


  def getRDD(lines: RDD[String]): RDD[WikipediaPageLink] = {
    logSampleData(lines, "WikipediaPageLink")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaPageLink Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaLinkTargetParser(elementFilter: ElementFilter[LinkTarget] = new DefaultElementFilter[LinkTarget])
  extends WikipediaElementParser[LinkTarget](elementFilter) {

  // Define a regex pattern to parse LinkTarget entries from the dump
  val ltRegex = """\((\d+),(\d+),'(.*?)'\)""".r

  def parseLine(lineInput: String): List[LinkTarget] = {
    val r = ltRegex.findAllIn(lineInput).matchData.toList
    r.map(m => LinkTarget(m.group(1).toInt, m.group(2).toInt, m.group(3)))
  }

  def defaultFilterElt(t: LinkTarget): Boolean = true

  def getRDD(lines: RDD[String]): RDD[LinkTarget] = {
    logSampleData(lines, "LinkTarget")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "LinkTarget Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class JoinedPageLinkParser(elementFilter: ElementFilter[JoinedPageLink] = new DefaultElementFilter[JoinedPageLink])
  extends WikipediaElementParser[JoinedPageLink](elementFilter) {

  // regex that matches the structure of JoinedPageLink
  val jplRegex = """\((\d+),(\d+),(\d+),'(.*?)',(\d+)\)""".r

  def parseLine(lineInput: String): List[JoinedPageLink] = {
    val r = jplRegex.findAllIn(lineInput).matchData.toList
    r.map(m => JoinedPageLink(
      m.group(1).toInt,
      m.group(2).toInt,
      m.group(3).toLong,
      m.group(4),
      m.group(5).toInt
    ))
  }

  def defaultFilterElt(t: JoinedPageLink): Boolean = {
    (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category) &&
      (t.fromNamespace == WikipediaNamespace.Page || t.fromNamespace == WikipediaNamespace.Category)
  }

  def getRDD(lines: RDD[String]): RDD[JoinedPageLink] = {
    logSampleData(lines, "JoinedPageLink")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "JoinedPageLink Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}



class WikipediaLangLinkParser(filter: ElementFilter[WikipediaLangLink] = new DefaultElementFilter[WikipediaLangLink])
  extends WikipediaElementParser[WikipediaLangLink](filter) {

  val llRegex = """\((\d+),'(.*?)','(.*?)'\)""".r

  def parseLine(lineInput: String): List[WikipediaLangLink] = {
    val r = llRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaLangLink(m.group(1).toInt, m.group(2), m.group(3).replace(" ", "_")))
  }

  def defaultFilterElt(t: WikipediaLangLink): Boolean = !t.title.isEmpty

  def getRDD(lines: RDD[String]): RDD[WikipediaLangLink] = {
    logSampleData(lines, "WikipediaLangLink")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaLangLink Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaRedirectParser(elementFilter: ElementFilter[WikipediaRedirect] = new DefaultElementFilter[WikipediaRedirect])
  extends WikipediaElementParser[WikipediaRedirect](elementFilter) {

  // Updated regex to handle 'rd_interwiki' and 'rd_fragment' fields
  val redirectRegex = """\((\d+),(\d+),'(.*?)',(NULL|''),(NULL|''|.*?)\)""".r

  def parseLine(lineInput: String): List[WikipediaRedirect] = {
    val r = redirectRegex.findAllIn(lineInput).matchData.toList
    r.map { m =>
      WikipediaRedirect(
        m.group(1).toInt,            // rd_from
        m.group(2).toInt,            // rd_namespace
        m.group(3),                  // rd_title
        if (m.group(4) == "NULL" || m.group(4) == "") None else Some(m.group(4)), // rd_interwiki: Option[String]
        if (m.group(5) == "NULL" || m.group(5) == "") None else Some(m.group(5))  // rd_fragment: Option[String]
      )
    }
  }

  def defaultFilterElt(t: WikipediaRedirect): Boolean =
    t.targetNamespace == WikipediaNamespace.Page || t.targetNamespace == WikipediaNamespace.Category

  def getRDD(lines: RDD[String]): RDD[WikipediaRedirect] = {
    logSampleData(lines, "WikipediaRedirect")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaRedirect Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}


class WikipediaCategoryParser(elementFilter: ElementFilter[WikipediaCategory] = new DefaultElementFilter[WikipediaCategory])
  extends WikipediaElementParser[WikipediaCategory](elementFilter) {

  val categoryRegex = """\((\d+),'(.*?)',(\d+),(\d+),(\d+)\)""".r

  def parseLine(lineInput: String): List[WikipediaCategory] = {
    val r = categoryRegex.findAllIn(lineInput).matchData.toList
    r.map(m => WikipediaCategory(m.group(1).toInt, m.group(2), m.group(3).toInt, m.group(4).toInt, m.group(5).toInt))
  }

  def defaultFilterElt(t: WikipediaCategory): Boolean = true

  def getRDD(lines: RDD[String]): RDD[WikipediaCategory] = {
    logSampleData(lines, "WikipediaCategory")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaCategory Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaCategoryLinkParser(elementFilter: ElementFilter[WikipediaCategoryLink] = new DefaultElementFilter[WikipediaCategoryLink])
  extends WikipediaElementParser[WikipediaCategoryLink](elementFilter) {

  //val categoryLinkRegex = """\((\d+),'(.*?)','(.*?)','(.*?)','(.*?)','(.*?)','(.*?)'\)""".r
  val categoryLinkRegex = """\((\d+),'([^']+)','(.*?)','(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})','([^']*)','([^']+)','([^']+)'\)""".r
  val timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def parseLine(lineInput: String): List[WikipediaCategoryLink] = {
    val r = categoryLinkRegex.findAllIn(lineInput).matchData.toList
    r.map { m =>
      WikipediaCategoryLink(
        m.group(1).toInt,   // cl_from
        m.group(2),         // cl_to
        m.group(3),         // cl_sortkey, we keep this as a raw string but handle it carefully
        new Timestamp(timestampFormat.parse(m.group(4)).getTime),  // cl_timestamp
        m.group(5),         // cl_sortkey_prefix
        m.group(6),         // cl_collation
        m.group(7)          // cl_type
      )
    }
  }

  def defaultFilterElt(t: WikipediaCategoryLink): Boolean = true

  def getRDD(lines: RDD[String]): RDD[WikipediaCategoryLink] = {
    logSampleData(lines, "WikipediaCategoryLink")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaCategoryLink Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaPagecountParser(elementFilter: ElementFilter[WikipediaPagecount] = new DefaultElementFilter[WikipediaPagecount])
  extends WikipediaElementParser[WikipediaPagecount](elementFilter) {

  val pageCountRegex = """^([a-z]{2}\.[a-z]+) (.*?) (\d+|null) (.*?) (\d+) ((?:[A-Z]\d+)+)$""".r
  val titleNsRegex = """(.*?):(.*?)""".r

  override def parseLine(lineInput: String): List[WikipediaPagecount] = {
    val r = pageCountRegex.findAllIn(lineInput).matchData.toList
    r.map(m => {
      val langCode = m.group(1).split('.')(0)
      val (title, nsStr) = m.group(2) match {
        case titleNsRegex(nsStr, title) => (title, nsStr)
        case _ => (m.group(2), "Page")
      }
      val ns = nsStr match {
        case "Page" => WikipediaNamespace.Page
        case "Category" => WikipediaNamespace.Category
        case "Book" => WikipediaNamespace.Book
        case _ => WikipediaNamespace.Dummy
      }
      WikipediaPagecount(langCode, m.group(2), ns, m.group(4), m.group(5).toInt, m.group(6))
    })
  }

  override def defaultFilterElt(t: WikipediaPagecount): Boolean = true

  override def getRDD(lines: RDD[String]): RDD[WikipediaPagecount] = {
    logSampleData(lines, "WikipediaPagecount")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaPagecount Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaPagecountLegacyParser(elementFilter: ElementFilter[WikipediaPagecount] = new DefaultElementFilter[WikipediaPagecount])
  extends WikipediaElementParser[WikipediaPagecount](elementFilter) {

  val pageCountRegex = """^([a-z]{2}\.[a-z]) (.*?) (\d+) ((?:[A-Z]\d+)+)$""".r
  val titleNsRegex = """(.*?):(.*?)""".r
  val urlEncodeRegex = """^%[A-F\d]{2}""".r

  def decodeTitle(t: String): String = {
    val d = t match {
      case urlEncodeRegex(_*) => URLDecoder.decode(t, StandardCharsets.UTF_8.name())
      case _ => t
    }
    d
  }

  def parseLine(lineInput: String): List[WikipediaPagecount] = {
    val r = pageCountRegex.findAllIn(lineInput).matchData.toList
    r.map(m => {
      val extTitle = decodeTitle(m.group(2))
      val (title, nsStr) = extTitle match {
        case titleNsRegex(nsStr, title) => (title, nsStr)
        case _ => (extTitle, "Page")
      }
      val ns = nsStr match {
        case "Page" => WikipediaNamespace.Page
        case "Category" => WikipediaNamespace.Category
        case "Book" => WikipediaNamespace.Book
        case _ => WikipediaNamespace.Dummy
      }
      val langCode = m.group(1).split('.')(0)
      WikipediaPagecount(langCode, title, ns, "web", m.group(3).toInt, m.group(4))
    })
  }

  def defaultFilterElt(t: WikipediaPagecount): Boolean = (t.namespace == WikipediaNamespace.Page || t.namespace == WikipediaNamespace.Category)

  def getRDD(lines: RDD[String]): RDD[WikipediaPagecount] = {
    logSampleData(lines, "WikipediaPagecountLegacy")
    val parsedRDD = lines.flatMap(l => parseLine(l))
    val filteredRDD = parsedRDD.filter(filterElt)
    logSampleData(filteredRDD.map(_.toString), "WikipediaPagecountLegacy Filtered")
    filteredRDD
  }

  def getDataFrame(session: SparkSession, data: RDD[String]): DataFrame = session.createDataFrame(getRDD(data))
}

class WikipediaHourlyVisitsParser extends Serializable {
  val visitRegex = """([A-Z])(\d+)""".r

  def parseField(input: String, date: LocalDate): List[WikipediaHourlyVisit] = {
    val r = visitRegex.findAllIn(input).matchData.toList
    r.map(m => {
      val hour = m.group(1).charAt(0).toInt - 'A'.toInt
      WikipediaHourlyVisit(LocalDateTime.of(date, LocalTime.of(hour, 0, 0)), m.group(2).toInt)
    })
  }
}
