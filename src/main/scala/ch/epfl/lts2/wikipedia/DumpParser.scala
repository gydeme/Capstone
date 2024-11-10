package ch.epfl.lts2.wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.lit
import org.rogach.scallop._
import java.nio.file.Paths

// Parser configuration using Scallop to manage CLI arguments
class ParserConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpFilePaths = opt[List[String]](required = true, name = "dumpFilePaths")
  val dumpType = opt[String](required = true, name = "dumpType")
  val outputPath = opt[String](required = true, name = "outputPath")
  val outputFormat = opt[String](name = "outputFormat", default = Some("csv"))
  val languages = opt[List[String]](name = "languages", default = Some(List()))
  verify()
}

class DumpParser extends Serializable with CsvWriter {

  def splitSqlInsertLine(line: String): String = {
    val insertPattern = "INSERT INTO .+ VALUES ".r
    insertPattern.replaceFirstIn(line, "").trim
  }

  def readCompressedFile(session: SparkSession, filePath: String): RDD[String] = {
    if (filePath.endsWith(".bz2")) {
      session.sparkContext.textFile(filePath, 4)
    } else {
      throw new IllegalArgumentException(s"Unsupported file format for compression: $filePath")
    }
  }

  def readCsvFile(session: SparkSession, filePath: String): DataFrame = {
    session.read.option("header", "true").csv(filePath)
  }

  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").option("compression", "gzip").parquet(outputPath)
  }

  // Process input based on the file type (CSV or .bz2)
  def processFileToDf[T <: WikipediaElement with Product](
                                                           session: SparkSession,
                                                           inputFilename: String,
                                                           dumpType: WikipediaDumpType.Value,
                                                           filter: ElementFilter[T] = new DefaultElementFilter[T]
                                                         ): DataFrame = {
    if (inputFilename.endsWith(".bz2")) {
      val lines = session.sparkContext.textFile(inputFilename, 4)
      processToDf[T](session, lines, dumpType, filter)
    } else if (inputFilename.endsWith(".csv")) {
      val csvData = readCsvFile(session, inputFilename)
      csvData
    } else {
      throw new IllegalArgumentException(s"Unsupported file format: $inputFilename")
    }
  }

  def processToDf[T <: WikipediaElement with Product](
                                                       session: SparkSession,
                                                       input: RDD[String],
                                                       dumpType: WikipediaDumpType.Value,
                                                       filter: ElementFilter[T] = new DefaultElementFilter[T]
                                                     ): DataFrame = {
    val sqlLines = input.filter(l => l.startsWith(s"INSERT INTO `$dumpType` VALUES"))
    val records = sqlLines.map(splitSqlInsertLine)
    val parser = dumpType match {
      case WikipediaDumpType.Page => new WikipediaPageParser
      case WikipediaDumpType.PageLinks => new WikipediaPageLinkParser
      case WikipediaDumpType.Redirect => new WikipediaRedirectParser
      case WikipediaDumpType.Category => new WikipediaCategoryParser
      case WikipediaDumpType.CategoryLinks => new WikipediaCategoryLinkParser
      case WikipediaDumpType.LangLinks => new WikipediaLangLinkParser(filter.asInstanceOf[ElementFilter[WikipediaLangLink]])
      case WikipediaDumpType.LinkTarget => new WikipediaLinkTargetParser
      case WikipediaDumpType.JoinedPageLink => new JoinedPageLinkParser
    }
    parser.getDataFrame(session, records)
  }

  def process[T <: WikipediaElement with Product](
                                                   session: SparkSession,
                                                   inputFilenames: List[String],
                                                   dumpType: WikipediaDumpType.Value,
                                                   elementFilter: ElementFilter[T],
                                                   outputPath: String,
                                                   outputFormat: String
                                                 ): Unit = {
    val df = inputFilenames
      .map(f => processFileToDf[T](session, f, dumpType, elementFilter).withColumn("languageCode", lit(splitFilename(f).langCode)))
      .reduce(_ union _)

    outputFormat match {
      case "parquet" => writeParquet(df, outputPath)
      case _ => writeCsv(df, outputPath, writeHeader = false, mode = "overwrite")
    }
  }

  def splitFilename(fileName: String): DumpInfo = {
    val p = Paths.get(fileName)
    val spl = p.getFileName.toString.split('-')
    val dt = spl(2).split('.')
    DumpInfo(spl(0).stripSuffix("wiki"), spl(1), dt(0))
  }
}

object DumpParser {
  val dumpParser = new DumpParser()

  def main(args: Array[String]): Unit = {
    val conf = new ParserConf(args)
    val dumpType = conf.dumpType()
    val outputFormat = conf.outputFormat()
    val languages = conf.languages()
    val sconf = new SparkConf().setAppName("Wikipedia dump parser")
    val session = SparkSession.builder.config(sconf).getOrCreate()
    assert(WikipediaNamespace.Page == 0)
    assert(WikipediaNamespace.Category == 14)
    val dumpEltType = dumpType match {
      case "page" => (WikipediaDumpType.Page, new DefaultElementFilter[WikipediaPage])
      case "pagelinks" => (WikipediaDumpType.PageLinks, new DefaultElementFilter[WikipediaPageLink])
      case "redirect" => (WikipediaDumpType.Redirect, new DefaultElementFilter[WikipediaRedirect])
      case "category" => (WikipediaDumpType.Category, new DefaultElementFilter[WikipediaCategory])
      case "categorylinks" => (WikipediaDumpType.CategoryLinks, new DefaultElementFilter[WikipediaCategoryLink])
      case "langlinks" => (WikipediaDumpType.LangLinks, new ElementFilter[WikipediaLangLink] {
        override def filterElt(t: WikipediaLangLink): Boolean = languages.isEmpty || languages.contains(t.lang)
      })
      case "linktarget" => (WikipediaDumpType.LinkTarget, new DefaultElementFilter[LinkTarget])
      case "joinedpagelink" => (WikipediaDumpType.JoinedPageLink, new DefaultElementFilter[JoinedPageLink])
    }

    dumpParser.process(session, conf.dumpFilePaths(), dumpEltType._1, dumpEltType._2, conf.outputPath(), conf.outputFormat())
  }
}
