package ch.epfl.lts2.wikipedia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time._
import java.sql.Timestamp
import org.apache.spark.sql.functions.lit
import com.typesafe.config.ConfigFactory

case class MergedPagecount (title: String, namespace: Int, visits: List[Visit], id: Int)

class PageCountSpec extends AnyFlatSpec with Matchers with SparkSessionTestWrapper with TestDataReal {

  "WikipediaHourlyVisitParser" should "parse hourly visits fields correctly" in {
    val p = new WikipediaHourlyVisitsParser
    val d = LocalDate.of(2018, 8, 1)
    val r = p.parseField("J1", d)
    r.size shouldEqual 1
    val j1 = r.head
    j1.time.getHour shouldEqual 9
    j1.visits shouldEqual 1

    val r2 = p.parseField("C1P3", d)
    r2.size shouldEqual 2
    val r2m = r2.map(w => (w.time.getHour, w.visits)).toMap
    r2m(2) shouldEqual 1
    r2m(15) shouldEqual 3

    val r3 = p.parseField("A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6", d)
    r3.size shouldEqual 24
    val r3m = r3.map(w => (w.time.getHour, w.visits)).toMap
    r3m(23) shouldEqual 6
    r3m(22) shouldEqual 15
    r3m(0) shouldEqual 9
    r3m(1) shouldEqual 13
  }

  "WikipediaPagecountParser" should "parse pagecounts (legacy) correctly" in {
    val langList = List("en")
    val enFilter = new ElementFilter[WikipediaPagecount] {
      override def filterElt(t: WikipediaPagecount): Boolean = langList.contains(t.languageCode)
    }
    val p = new WikipediaPagecountLegacyParser(enFilter)

    val pcLines = pageCountLegacy.split('\n').filter(p => !p.startsWith("#"))

    val rdd = p.getRDD(spark.sparkContext.parallelize(pcLines, 2))
    val resMap = rdd.map(w => (w.title, w)).collect().toMap

    resMap.keys.size shouldEqual 20

    val res16 = resMap("16th_amendment")
    res16.namespace shouldEqual WikipediaNamespace.Page
    res16.dailyVisits shouldEqual 2
    res16.hourlyVisits shouldEqual "C1P1"
    val res16c = resMap("16th_ammendment")
    res16c.namespace shouldEqual WikipediaNamespace.Category
    res16c.dailyVisits shouldEqual 1
    res16c.hourlyVisits shouldEqual "J1"
    resMap.get("16th_century_in_literature") shouldBe None // book -> filtered out

    val res16th = resMap("16th_century")
    res16th.dailyVisits shouldEqual 258
    res16th.namespace shouldEqual WikipediaNamespace.Page
    res16th.hourlyVisits shouldEqual "A9B13C9D15E7F14G9H8I8J9K8L4M9N18O9P15Q17R12S10T12U7V15W15X6"
  }

  "WikipediaPagecountParser" should "parse pageviews correctly" in {
    val langList = List("en")
    val enFilter = new ElementFilter[WikipediaPagecount] {
      override def filterElt(t: WikipediaPagecount): Boolean = langList.contains(t.languageCode)
    }
    val p = new WikipediaPagecountParser(enFilter)

    val pcLines = pageViews.split('\n').filter(p => !p.startsWith("#"))

    val rdd = p.getRDD(spark.sparkContext.parallelize(pcLines, 2))
    val resMap = rdd.map(w => (w.title, w)).collect().toMap

    resMap.keys.size shouldEqual 23
    val res = resMap("ABC_(song)")
    res.namespace shouldEqual WikipediaNamespace.Page
    res.dailyVisits shouldEqual 3
    res.hourlyVisits shouldEqual "L2Q1"
  }

  "PagecountProcessor" should "generate correct date ranges" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(), ConfigFactory.parseString(""), false, false)
    val range = p.dateRange(LocalDate.parse("2018-08-01"), LocalDate.parse("2018-08-10"), Period.ofDays(1))
    range.size shouldEqual 10
    val r2 = p.dateRange(LocalDate.parse("2017-08-01"), LocalDate.parse("2017-09-01"), Period.ofDays(1))
    r2.size shouldEqual 32
    assertThrows[IllegalArgumentException](p.dateRange(LocalDate.parse("2017-09-01"), LocalDate.parse("2017-01-01"), Period.ofDays(1)))
  }

  it should "update correctly pagecount metadata" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(), ConfigFactory.parseString(""), false, false)
    val d1 = LocalDate.of(2018, 6, 1)
    val d2 = LocalDate.of(2018, 5, 1)
    val d3 = LocalDate.of(2018, 6, 30)
    val d3r = LocalDate.of(2018, 7, 1)
    val midnight = LocalTime.MIDNIGHT
    val tsref1 = Timestamp.valueOf(LocalDateTime.of(d1, midnight))
    val tsref2 = Timestamp.valueOf(LocalDateTime.of(d2, midnight))
    val tsref3 = Timestamp.valueOf(LocalDateTime.of(d3, midnight))
    val tsref3r = Timestamp.valueOf(LocalDateTime.of(d3r, midnight))
    val ts1 = p.getLatestDate(tsref1, d2)
    ts1 shouldEqual tsref1
    val ts2 = p.getLatestDate(tsref1, d3)
    ts2 shouldEqual tsref3r
    val ts3 = p.getEarliestDate(tsref1, d3)
    ts3 shouldEqual tsref1
    val ts4 = p.getEarliestDate(tsref1, d2)
    ts4 shouldEqual tsref2
  }

  it should "read correctly pagecounts (legacy)" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(), ConfigFactory.parseString(""), false, true)
    val rdd = p.parseLines(spark.sparkContext.parallelize(pageCountLegacy2, 2), 100, 2000, LocalDate.of(2018, 8, 1))
    val refTime = Timestamp.valueOf(LocalDate.of(2018, 8, 1).atStartOfDay)
    val res1 = rdd.filter(f => f.title == "Anarchism").collect()
    val res2 = rdd.filter(f => f.title == "AfghanistanHistory").collect()
    val res3 = rdd.filter(f => f.title == "AnchorageAlaska").collect()
    res1.size shouldEqual 1
    val ref1 = Visit(refTime, 300, "Day")
    res1(0).namespace shouldEqual WikipediaNamespace.Category
    res1(0).visits.size shouldEqual 1
    res1(0).visits.contains(ref1) shouldBe true
    res2.size shouldEqual 1
    val ref2 = Visit(refTime, 200, "Day")
    res2(0).namespace shouldEqual WikipediaNamespace.Page
    res2(0).visits.size shouldEqual 1
    res2(0).visits.contains(ref2) shouldBe true
    res3.size shouldEqual 1
    res3(0).namespace shouldEqual WikipediaNamespace.Page
    res3(0).visits.size shouldEqual 5
    res3(0).visits.map(p => assert(p.count == 600 && p.timeResolution == "Hour"))
  }

  it should "read and filter correctly pageviews" in {
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountParser(), ConfigFactory.parseString(""), false, false)
    val rdd = p.parseLines(spark.sparkContext.parallelize(pageViews2, 2), 100, 200, LocalDate.of(2020, 12, 1))
    val refTime = Timestamp.valueOf(LocalDate.of(2020, 12, 1).atStartOfDay)
    val res1 = rdd.filter(f => f.title == "ABC_(The_Jackson_5_song)").collect()
    val res2 = rdd.filter(f => f.title == "ABC_(band)").collect()
    val res3 = rdd.filter(f => f.title == "ABC_2000_Today").collect()
    res1.size shouldEqual 1
    val ref1 = Visit(refTime, 174, "Day")
    res1(0).namespace shouldEqual WikipediaNamespace.Page
    res1(0).visits.size shouldEqual 1
    res1(0).visits.contains(ref1) shouldBe true
    res2.size shouldEqual 1
    res2(0).namespace shouldEqual WikipediaNamespace.Page
    res2(0).visits.size shouldEqual 24
    res2(0).visits(0).count shouldEqual 19
    res2(0).visits(0).timeResolution shouldEqual "Hour"
    res2(0).visits(23).count shouldEqual 17
    res2(0).visits(23).timeResolution shouldEqual "Hour"
    res3.isEmpty shouldBe true
  }

  it should "filter according to minimum daily visits correctly" in {
    import spark.implicits._
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(), ConfigFactory.parseString(""), false, true)
    val pcDf = p.parseLinesToDf(spark.sparkContext.parallelize(pageCountLegacy2, 2), 150, 2000, LocalDate.of(2018, 8, 1))
    pcDf.count() shouldEqual 3
    val res = pcDf.filter(p => p.title == "AccessibleComputing").collect()
    res.isEmpty shouldBe true
  }

  it should "merge page dataframe correctly" in {
    import spark.implicits._
    val p = new PagecountProcessor(List("en"), new WikipediaPagecountLegacyParser(), ConfigFactory.parseString(""), false, true)
    val pcDf = p.parseLinesToDf(spark.sparkContext.parallelize(pageCountLegacy2, 2), 100, 2000, LocalDate.of(2018, 8, 1))
    val dp = new DumpParser
    val df = dp.processToDf(spark, spark.sparkContext.parallelize(Seq(sqlPage), 2), WikipediaDumpType.Page)
    val res = p.mergePagecount(df.withColumn("languageCode", lit("en")).as[WikipediaPageLang], pcDf).as[MergedPagecount]
    val res1 = res.filter(p => p.title == "Anarchism").collect()
    val res2 = res.filter(p => p.title == "AfghanistanHistory").collect()
    val res3 = res.filter(f => f.title == "AnchorageAlaska").collect()
    res1.size shouldEqual 1
    res1(0).id shouldEqual 12
    res2.size shouldEqual 1
    res2(0).id shouldEqual 13
    res3.size shouldEqual 1
    res3(0).id shouldEqual 258
  }
}

