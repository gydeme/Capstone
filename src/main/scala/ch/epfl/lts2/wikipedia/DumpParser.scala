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

class DumpParser extends Serializable {

//  // Helper method to split INSERT INTO SQL lines
//  def splitSqlInsertLine(line: String): String = {
//    line.split(" VALUES ")(1).trim
//  }

  def splitSqlInsertLine(line: String): String = {
    // Regular expression to capture everything after the VALUES keyword
    val insertPattern = "INSERT INTO .+ VALUES ".r
    // Use regex to replace the "INSERT INTO `table` VALUES" with an empty string, leaving only the rows
    val result = insertPattern.replaceFirstIn(line, "").trim
    // Return the rows part (result should start with the opening parenthesis for the rows)
    result
  }


  // Reads .bz2 compressed files using Spark
  def readCompressedFile(session: SparkSession, filePath: String): RDD[String] = {
    if (filePath.endsWith(".bz2")) {
      session.sparkContext.textFile(filePath, 4) // Spark handles the bz2 compression
    } else {
      throw new IllegalArgumentException(s"Unsupported file format for compression: $filePath")
    }
  }

  // Reads CSV files using Spark
  def readCsvFile(session: SparkSession, filePath: String): DataFrame = {
    session.read.option("header", "true").csv(filePath)
  }

  // Writes DataFrame to Parquet format
  def writeParquet(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").option("compression", "gzip").parquet(outputPath)
  }

  // Writes DataFrame to CSV format
  def writeCsv(df: DataFrame, outputPath: String): Unit = {
    df.write.mode("overwrite").option("header", "true").csv(outputPath)
  }

  // Process input based on the file type (CSV or .bz2)
  def processFileToDf[T <: WikipediaElement with Product](session: SparkSession, inputFilename: String, dumpType: WikipediaDumpType.Value,
                                                          filter: ElementFilter[T] = new DefaultElementFilter[T]): DataFrame = {
    if (inputFilename.endsWith(".bz2")) {
      val lines = session.sparkContext.textFile(inputFilename, 4)
      processToDf[T](session, lines, dumpType, filter)
    } else if (inputFilename.endsWith(".csv")) {
      val csvData = readCsvFile(session, inputFilename)
      csvData // Assuming the CSV already follows the desired schema; modify as needed
    } else {
      throw new IllegalArgumentException(s"Unsupported file format: $inputFilename")
    }
  }

  // Processes .bz2 files into a DataFrame
  def processToDf[T <: WikipediaElement with Product](session: SparkSession, input: RDD[String], dumpType: WikipediaDumpType.Value,
                                                      filter: ElementFilter[T] = new DefaultElementFilter[T]): DataFrame = {

    val sqlLines = input.filter(l => l.startsWith("INSERT INTO `%s` VALUES".format(dumpType)))
    val records = sqlLines.map(l => splitSqlInsertLine(l))
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

  // Main process function: handles both CSV and .bz2 files
  def process[T <: WikipediaElement with Product](session: SparkSession, inputFilenames: List[String],
                                                  dumpType: WikipediaDumpType.Value, elementFilter: ElementFilter[T],
                                                  outputPath: String, outputFormat: String): Unit = {
    val df = inputFilenames.map(f => processFileToDf[T](session, f, dumpType, elementFilter)
        .withColumn("languageCode", lit(splitFilename(f).langCode)))
      .reduce((p1, p2) => p1.union(p2))

    outputFormat match {
      case "parquet" => writeParquet(df, outputPath)
      case _ => writeCsv(df, outputPath)
    }
  }

  // Split the filename to extract metadata
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
