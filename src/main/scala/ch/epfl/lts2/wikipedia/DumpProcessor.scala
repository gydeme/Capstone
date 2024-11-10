package ch.epfl.lts2.wikipedia

import java.nio.file.{Files, Paths, StandardOpenOption}
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.rogach.scallop._
import org.apache.log4j.{Level, Logger}

class ProcessorConf(args: Seq[String]) extends ScallopConf(args) with Serialization {
  val dumpPath = opt[String](required = true, name = "dumpPath")
  val outputPath = opt[String](required = true, name = "outputPath")
  val namePrefix = opt[String](required = true, name = "namePrefix")
  //val handleRowCompression = opt[Boolean](name = "handleRowCompression", default = Some(true))
  // val dumpType = opt[String](name = "dumpType", required = false)
  verify()
}

class DumpProcessor extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger("ch.epfl.lts2.wikipedia.DumpProcessor")
  lazy val sconf = new SparkConf().setAppName("Wikipedia dump processor")
//    .set("spark.executor.memory", "20g")
//    .set("spark.driver.memory", "20g")
//    //.set("spark.sql.shuffle.partitions", "2000")
      .setMaster("local[*]")  // Set the master URL explicitly
  lazy val session: SparkSession = SparkSession.builder.config(sconf).getOrCreate()

  // Import session implicits to bring in the encoders
  import session.implicits._

  implicit val wikipediaPageEncoder = Encoders.product[WikipediaPage]
  implicit val wikipediaPageLinkEncoder = Encoders.product[WikipediaPageLink]
  implicit val wikipediaCategoryLinkEncoder = Encoders.product[WikipediaCategoryLink]
  implicit val wikipediaRedirectEncoder = Encoders.product[WikipediaRedirect]
  implicit val wikipediaSimplePageEncoder = Encoders.product[WikipediaSimplePage]
  implicit val joinedPageLinkEncoder = Encoders.product[JoinedPageLink]

  def getCurrentTimestamp(): String = {
    new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date())
  }

//  def writeToFile(content: String, filePath: String): Unit = {
//    // Files.write(Paths.get(filePath), content.getBytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND)
//  }
//
//  def inspectRDD(rdd: org.apache.spark.rdd.RDD[String], name: String, rddFilePath: String): Unit = {
//    //    val sampleData = rdd.filter(!_.startsWith("--")).filter(!_.startsWith("/*!")).filter(_.nonEmpty).take(10).mkString("\n")
//    //    val content = s"RDD: $name\nSample Data:\n$sampleData\n"
//    //    writeToFile(content, rddFilePath)
//  }

//  def inspectDataset[T](ds: Dataset[T], name: String, outputFilePath: String): Unit = {
//    //    val schemaInfo = ds.schema.treeString
//    //    val rowCount = ds.count()
//    //    val sampleData = ds.take(10).mkString("\n")
//    //    val content = s"Dataset: $name\nSchema:\n$schemaInfo\nCount: $rowCount\nSample Data:\n$sampleData\n\n"
//    //    writeToFile(content, outputFilePath)
//    //    if (rowCount == 0) {
//    //      logger.error(s"Dataset $name has zero rows!")
//    //    }
//    // Dummy operation to replace the actual functionality
//    val placeholder: Boolean = true
//  }

  // Method to join pageLinks with linkTarget data and produce JoinedPageLink
  def createJoinedPageLink(pageLinkDf: Dataset[WikipediaPageLink], linkTargetDf: Dataset[LinkTarget], outputFilePath: String): Dataset[JoinedPageLink] = {
    val result = pageLinkDf.join(linkTargetDf, pageLinkDf("targetId") === linkTargetDf("lt_id"))
      .select(
        pageLinkDf("from"),
        pageLinkDf("fromNamespace"),
        pageLinkDf("targetId"),
        linkTargetDf("lt_title").as("title"),  // Restored title from linktarget
        linkTargetDf("lt_namespace").as("namespace")  // Restored namespace from linktarget
      )
      .as[JoinedPageLink]

    //result.show(10)  // Show 10 rows for debugging purposes
    result
  }


  def mergePageLink(pageDf: Dataset[WikipediaPage], joinedPageLinkDf: Dataset[JoinedPageLink], outputFilePath: String): Dataset[MergedPageLink] = {
    logger.info("Starting mergePageLink")
    import session.implicits._
//    inspectDataset(pageDf, "pageDf_before_mergePageLink", outputFilePath)
//    inspectDataset(joinedPageLinkDf, "joinedPageLinkDf_before_mergePageLink", outputFilePath)

    val result = joinedPageLinkDf.join(pageDf, Seq("title", "namespace"))
      .select("from", "id", "title", "fromNamespace", "namespace")
      .as[MergedPageLink]

    //    result.show(10)
    //    logger.info(s"mergePageLink result count: ${result.count()}")
    result
  }

  def mergeRedirect(pageDf: Dataset[WikipediaPage], redirectDf: Dataset[WikipediaRedirect], outputFilePath: String): Dataset[MergedRedirect] = {
    import session.implicits._
    logger.info("Starting mergeRedirect")
//    inspectDataset(pageDf, "pageDf_before_mergeRedirect", outputFilePath)
//    inspectDataset(redirectDf, "redirectDf_before_mergeRedirect", outputFilePath)

    val result = redirectDf.withColumn("id", redirectDf.col("from"))
      .join(pageDf.drop(pageDf.col("title")), "id")
      .select("from", "targetNamespace", "title")
      .withColumn("namespace", redirectDf.col("targetNamespace"))
      .join(pageDf, Seq("title", "namespace"))
      .select("from", "id", "title")
      .as[MergedRedirect]

    //    result.show(10)
    //    logger.info(s"mergeRedirect result count: ${result.count()}")
    result
  }

  def mergeCategoryLinks(pageDf: Dataset[WikipediaPage], categoryPageDf: Dataset[WikipediaSimplePage], catLinkDf: Dataset[WikipediaCategoryLink], outputFilePath: String): Dataset[MergedCatlink] = {
    import session.implicits._
    logger.info("Starting mergeCategoryLinks")
//    inspectDataset(pageDf, "pageDf_before_mergeCategoryLinks", outputFilePath)
//    inspectDataset(categoryPageDf, "categoryPageDf_before_mergeCategoryLinks", outputFilePath)
//    inspectDataset(catLinkDf, "catLinkDf_before_mergeCategoryLinks", outputFilePath)

    val result = catLinkDf.withColumn("id", catLinkDf.col("from"))
      .join(pageDf, "id")
      .select("from", "to", "ctype")
      .withColumn("title", catLinkDf.col("to"))
      .join(categoryPageDf.select("id", "title"), "title")
      .select("from", "title", "id", "ctype")
      .as[MergedCatlink]

    //    result.show(10)
    //    logger.info(s"mergeCategoryLinks result count: ${result.count()}")
    // inspectDataset(result, "catlinks_id", outputFilePath)
    result
  }

  def getPagesByNamespace(pageDf: Dataset[WikipediaPage], ns: Int, keepRedirect: Boolean, outputFilePath: String): Dataset[WikipediaSimplePage] = {
    import session.implicits._
    logger.info(s"Starting getPagesByNamespace with namespace $ns and keepRedirect $keepRedirect")
    //inspectDataset(pageDf, "pageDf_before_getPagesByNamespace", outputFilePath)

    val result = pageDf.filter(p => p.namespace == ns && (keepRedirect || !p.isRedirect))
      .select("id", "title", "isRedirect", "isNew").as[WikipediaSimplePage]

    //    result.show(10)
    //    logger.info(s"getPagesByNamespace result count: ${result.count()}")
    // inspectDataset(result, "pages_in_namespace", outputFilePath)
    result
  }

  def resolvePageRedirects(pgLinksIdDf: Dataset[MergedPageLink], redirectDf: Dataset[MergedRedirect], pages: Dataset[WikipediaSimplePage], outputFilePath: String): DataFrame = {
    logger.info("Starting resolvePageRedirects")
    import session.implicits._
    //    inspectDataset(pgLinksIdDf, "pgLinksIdDf_before_resolvePageRedirects", outputFilePath)
//    inspectDataset(redirectDf, "redirectDf_before_resolvePageRedirects", outputFilePath)
//    inspectDataset(pages, "pages_before_resolvePageRedirects", outputFilePath)

    val linksDf = pgLinksIdDf.withColumn("inter", pgLinksIdDf.col("id"))
      .join(redirectDf.withColumn("inter", redirectDf.col("from")).withColumnRenamed("from", "from_r").withColumnRenamed("id", "to_r"), Seq("inter"), "left")
      .withColumn("dest", when(col("to_r").isNotNull, col("to_r")).otherwise(col("id")))
      .select("from", "dest")
      .filter($"from" =!= $"dest")
      .distinct

    linksDf.withColumn("id", linksDf.col("from"))
      .join(pages, "id")
      .filter($"isRedirect" === false)
      .select("from", "dest")
  }
}

object DumpProcessor {

  val dp = new DumpProcessor

  def main(args: Array[String])= {
    import dp.session.implicits._
    val conf = new ProcessorConf(args)
    val dumpParser = new DumpParser
    // logging vals
    //dp.logger.info("Starting main process")

    val inputOutputPath = conf.outputPath()
    //val rddFilePath = Paths.get(inputOutputPath, s"${dp.getCurrentTimestamp()}_input_rdd_info.txt").toString
    val outputFilePath = Paths.get(inputOutputPath, s"${dp.getCurrentTimestamp()}_output_dataset_info.txt").toString

    // Load the datasets
    val pageFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-page.sql.bz2").toString
    val pageOutput = Paths.get(conf.outputPath(), "page")
    //val pageDf = dumpParser.processFileToDf(dp.session, pageFile, WikipediaDumpType.Page).as[WikipediaPage].repartition(200)
    val pageDf = dumpParser.processFileToDf(dp.session, pageFile, WikipediaDumpType.Page).as[WikipediaPage]

    //pageDf.cache().count()
    //pageDf.show(20, false)
    println("page")


    val pageLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-pagelinks.sql.bz2").toString
    val pageLinksOutput = Paths.get(conf.outputPath(), "pagelinks").toString
    val pageLinksDf = dumpParser.processFileToDf(dp.session, pageLinksFile, WikipediaDumpType.PageLinks).as[WikipediaPageLink]

    //pageLinksDf.cache().count()
   // pageLinksDf.show(20, false)
    println("pagelinks")

    val categoryLinksFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-categorylinks.sql.bz2").toString
    val categoryLinksOutput = Paths.get(conf.outputPath(), "categorylinks").toString
    val categoryLinksDf = dumpParser.processFileToDf(dp.session, categoryLinksFile, WikipediaDumpType.CategoryLinks).as[WikipediaCategoryLink]

    //categoryLinksDf.cache().count()
    //categoryLinksDf.show(20, false)
    println("categorylinks")

    val redirectFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-redirect.sql.bz2").toString
    val redirectOutput = Paths.get(conf.outputPath(), "redirect").toString
    val redirectDf = dumpParser.processFileToDf(dp.session, redirectFile, WikipediaDumpType.Redirect).as[WikipediaRedirect]

    //redirectDf.cache().count()
   // redirectDf.show(20, false)
    println("redirect")

    // If using the linkTarget dataset
    val linkTargetFile = Paths.get(conf.dumpPath(), conf.namePrefix() + "-linktarget.sql.bz2").toString
    val linkTargetDf = dumpParser.processFileToDf(dp.session, linkTargetFile, WikipediaDumpType.LinkTarget).as[LinkTarget]

    //linkTargetDf.cache().count()
   // linkTargetDf.show(20, false)
    println("linktarget")

    // Manual join to create JoinedPageLink
    val joinedPageLinkDf = dp.createJoinedPageLink(pageLinksDf, linkTargetDf, outputFilePath)
    //joinedPageLinkDf.cache().count()
    //joinedPageLinkDf.show(20, false)
    println("joinedpagelink")

    // Merging and processing
    val pagelinks_id = dp.mergePageLink(pageDf, joinedPageLinkDf, outputFilePath)
    val normal_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Page, false, outputFilePath)
    val cat_pages = dp.getPagesByNamespace(pageDf, WikipediaNamespace.Category, false, outputFilePath)
    val redirect_id = dp.mergeRedirect(pageDf, redirectDf, outputFilePath)
    val pglinks_noredirect = dp.resolvePageRedirects(pagelinks_id, redirect_id, normal_pages.union(cat_pages), outputFilePath)

    val catlinks_id = dp.mergeCategoryLinks(pageDf, cat_pages, categoryLinksDf, outputFilePath)
    //dumpParser.writeCsv(pagelinks_id.toDF, pageLinksOutput)




    // Writing the output datasets to the respective output paths
    dumpParser.writeCsv(pglinks_noredirect, pageLinksOutput)
    dumpParser.writeCsv(catlinks_id.toDF, categoryLinksOutput)
    dumpParser.writeCsv(cat_pages.toDF, pageOutput.resolve("category_pages").toString)
    dumpParser.writeCsv(normal_pages.toDF, pageOutput.resolve("normal_pages").toString)

  }
}
