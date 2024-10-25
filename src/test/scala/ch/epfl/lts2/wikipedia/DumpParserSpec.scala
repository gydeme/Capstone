package ch.epfl.lts2.wikipedia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, Row, DataFrame, SparkSession}

class DumpParserSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper with TestDataReal {

  "DumpParser" should "split sql insert statements correctly" in {
    val dp = new DumpParser
    val res = dp.splitSqlInsertLine(sqlPage)
    res shouldEqual expectPage

    dp.splitSqlInsertLine(sqlPageLong) shouldEqual expectPageLong
    assertThrows[ArrayIndexOutOfBoundsException](dp.splitSqlInsertLine(expectPage)) // no insert statement -> throw
  }

  it should "parse page insert statement correctly into dataframe" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 2), WikipediaDumpType.Page)

    val res = df.as[WikipediaPage].collect().map(f => (f.id, f)).toMap
    res.keys.size shouldEqual 4
    val r10 = res(10)
    r10.namespace shouldEqual 0
    r10.title shouldEqual "AccessibleComputing"
    r10.isNew shouldEqual false
    r10.isRedirect shouldEqual true

    val r12 = res(12)
    r12.namespace shouldEqual 14
    r12.title shouldEqual "Anarchism"
    r12.isNew shouldEqual false
    r12.isRedirect shouldEqual false

    val r13 = res(13)
    r13.namespace shouldEqual 0
    r13.title shouldEqual "AfghanistanHistory"
    r13.isNew shouldEqual false
    r13.isRedirect shouldEqual true

    val r258 = res(258)
    r258.namespace shouldEqual 0
    r258.title shouldEqual "AnchorageAlaska"
    r258.isNew shouldEqual false
    r258.isRedirect shouldEqual true
  }

  it should "parse long page insert statement correctly into dataframe" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLong), 2), WikipediaDumpType.Page)

    val res = df.as[WikipediaPage].collect().map(f => (f.id, f)).toMap
    res.keys.size shouldEqual 9 // namespace == 1 are filtered out, only 0 and 14 are kept
    val pnrCat = res(45533)
    pnrCat.namespace shouldEqual 14
    pnrCat.title shouldEqual "Pseudorandom_number_generator"
    pnrCat.isNew shouldEqual false
    pnrCat.isRedirect shouldEqual false

    val socDar = res(45541)
    socDar.namespace shouldEqual 0
    socDar.title shouldEqual "Social_Darwinism"
    socDar.isNew shouldEqual false
    socDar.isRedirect shouldEqual false

    val refuge = res(45546)
    refuge.namespace shouldEqual 0
    refuge.title shouldEqual "Refugees"
    refuge.isNew shouldEqual false
    refuge.isRedirect shouldEqual true
  }

  it should "parse pagelinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPageLinks), 2), WikipediaDumpType.PageLinks)
    val res = df.as[WikipediaPageLink].collect().map(f => (f.from, f)).toMap

    res.keys.size shouldEqual 44
    res.values.map(v => assert(v.fromNamespace == 0 && v.namespace == 0 && v.title == "1000_in_Japan"))
  }

  it should "parse redirect insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlRedirect), 2), WikipediaDumpType.Redirect)
    val res = df.as[WikipediaRedirect].collect().map(f => (f.from, f)).toMap
    res.keys.size shouldEqual 11
    res.values.map(v => assert(v.targetNamespace == 0))
    val am = res(24)
    am.title shouldEqual "Amoeba"
    val haf = res(13)
    haf.title shouldEqual "History_of_Afghanistan"
    val at = res(23)
    at.title shouldEqual "Assistive_technology"
  }

  it should "parse category insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlCategory), 2), WikipediaDumpType.Category)
    val res = df.as[WikipediaCategory].collect().map(f => (f.id, f)).toMap
    res.keys.size shouldEqual 17
    val sdg = res(388205)
    sdg.title shouldEqual "Swiss_death_metal_musical_groups"
    val pbg = res(388207)
    pbg.title shouldEqual "Peruvian_black_metal_musical_groups"
    val cs = res(388222)
    cs.title shouldEqual "Culture_in_Cheshire"
  }

  it should "parse categorylinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlCatLink), 2), WikipediaDumpType.CategoryLinks)
    val res = df.as[WikipediaCategoryLink].collect().map(f => (f.from, f))
    res.size shouldEqual 7
    res.filter(p => p._2.to == "Oxford_University_Press_people").size shouldEqual 1
    res.filter(p => p._2.to == "English_male_short_story_writers").size shouldEqual 1
    res.filter(p => p._2.to == "Pages_using_citations_with_format_and_no_URL").size shouldEqual 1
  }

  it should "split filenames correctly" in {
    val dp = new DumpParser
    val di = dp.splitFilename("enwiki-20180801-langlinks.sql.bz2")
    di.langCode shouldEqual "en"
    di.dateCode shouldEqual "20180801"
    di.dumpType shouldEqual "langlinks"
    val dii = dp.splitFilename("frwiki-20190901-page.sql.bz2")
    dii.langCode shouldEqual "fr"
    dii.dateCode shouldEqual "20190901"
    dii.dumpType shouldEqual "page"
    val dii_small = dp.splitFilename("frwiki-20190901-page.small.sql.bz2")
    dii_small.langCode shouldEqual "fr"
    dii_small.dateCode shouldEqual "20190901"
    dii_small.dumpType shouldEqual "page"
  }

  it should "parse langlinks insert statements correctly" in {
    import spark.implicits._
    val dp = new DumpParser
    val frFilter = new ElementFilter[WikipediaLangLink] {
      override def filterElt(t: WikipediaLangLink): Boolean = t.lang == "fr"
    }
    val esFilter = new ElementFilter[WikipediaLangLink] {
      override def filterElt(t: WikipediaLangLink): Boolean = t.lang == "es"
    }
    val dfr = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksFr), 2), WikipediaDumpType.LangLinks, frFilter)
    val resfr = dfr.as[WikipediaLangLink].collect()
    resfr.length shouldEqual 15

    val dfr2 = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksFr2), 2), WikipediaDumpType.LangLinks, frFilter)
    val resfr2 = dfr2.as[WikipediaLangLink].collect()
    resfr2.length shouldEqual 2 // test if empty entries are filtered out correctly

    val desEmpt = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksEs), 2), WikipediaDumpType.LangLinks, frFilter)
    val resesEmpt = desEmpt.as[WikipediaLangLink].collect()
    resesEmpt.isEmpty shouldEqual true

    val des = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(langLinksEs), 2), WikipediaDumpType.LangLinks, esFilter)
    val reses = des.as[WikipediaLangLink].collect()
    reses.length shouldEqual 11
  }
}

