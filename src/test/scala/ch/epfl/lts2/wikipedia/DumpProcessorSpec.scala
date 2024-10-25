package ch.epfl.lts2.wikipedia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DumpProcessorSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper with TestDataReal {
  spark.sparkContext.setLogLevel("WARN")

  "DumpProcessor" should "filter pages according to namespace correctly" in {
    val dproc = new DumpProcessor
    val dp = new DumpParser
    import spark.implicits._
    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    pages.count shouldEqual 4
    val normal_pages = dproc.getPagesByNamespace(pages, WikipediaNamespace.Page, true)
    val category_pages = dproc.getPagesByNamespace(pages, WikipediaNamespace.Category, true)
    normal_pages.count shouldEqual 3
    category_pages.count shouldEqual 1
  }

  val sqlPageLinkDummy = "INSERT INTO `pagelinks` VALUES (10,14,'Anarchism',0),(12,0,'AfghanistanHistory',14),(13,0,'Nonexisting_Page',0);"
  it should "join page and pagelinks correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser

    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page)
    val pageLink = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLinkDummy), 1), WikipediaDumpType.PageLinks)
    pageLink.count shouldEqual 3

    val jdf = dproc.mergePageLink(pages.as[WikipediaPage], pageLink.as[WikipediaPageLink])
    val jdp = jdf.collect().map(w => (w.from, w)).toMap
    jdp.keys.size shouldEqual 2 // should ignore references to non-existing pages
    val pl1 = jdp(10)
    pl1.fromNamespace shouldEqual 0
    pl1.id shouldEqual 12
    pl1.namespace shouldEqual 14
    val pl2 = jdp(12)
    pl2.fromNamespace shouldEqual 14
    pl2.id shouldEqual 13
    pl2.namespace shouldEqual 0
  }

  val sqlRedirectDummy = "INSERT INTO `redirect` VALUES (12,0,'AccessibleComputing','n/a','n/a'),(13,0,'Non_existing_page',NULL,NULL);"
  it should "join page and redirect correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser

    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    val red = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlRedirectDummy), 1), WikipediaDumpType.Redirect).as[WikipediaRedirect]
    red.count shouldEqual 2
    val rds = dproc.mergeRedirect(pages, red).collect().map(w => (w.from, w)).toMap
    rds.keys.size shouldEqual 1
    val r1 = rds(12)
    r1.id shouldEqual 10
  }

  val catLinksSqlDummy = "INSERT INTO `categorylinks` VALUES (10,'Anarchism','','2018-08-01 02:00:00','','','page'),(13,'Dummy_category','','1970-01-01 00:00:00','','','subcat'); "
  it should "join page and categorylinks correctly" in {
    import spark.implicits._
    val dproc = new DumpProcessor
    val dp = new DumpParser

    val pages = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 1), WikipediaDumpType.Page).as[WikipediaPage]
    val cl = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(catLinksSqlDummy), 1), WikipediaDumpType.CategoryLinks).as[WikipediaCategoryLink]
    cl.count shouldEqual 2

    val cp = dproc.getPagesByNamespace(pages, WikipediaNamespace.Category, true)
    val cldf = dproc.mergeCategoryLinks(pages, cp, cl).collect().map(w => (w.from, w)).toMap
    cldf.keys.size shouldEqual 1
    val cl1 = cldf(10)
    cl1.id shouldEqual 12
    cl1.title shouldEqual "Anarchism"
    cl1.ctype shouldEqual "page"
  }
}

