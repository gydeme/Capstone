package ch.epfl.lts2.wikipedia

import java.sql.Timestamp
import java.time._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class PageCountStatsSpec extends AnyFlatSpec with Matchers with DateChecker {

  "PageCountStatsLoader" should "check date ranges correctly" in {
    val startDate = LocalDate.of(2018, 9, 1)
    val endDate = LocalDate.of(2018, 9, 30)
    val startTime = Timestamp.valueOf(startDate.atStartOfDay)
    val endTime = Timestamp.valueOf(endDate.atStartOfDay)
    val metadata = Array(PagecountMetadata(startTime, endTime))
    checkDateRange(metadata, LocalDate.of(2018, 9, 2), LocalDate.of(2018, 9, 4)) shouldEqual true
    checkDateRange(metadata, LocalDate.of(2018, 9, 1), LocalDate.of(2018, 9, 30)) shouldEqual true
    checkDateRange(metadata, LocalDate.of(2018, 9, 20), LocalDate.of(2018, 10, 2)) shouldEqual false
    checkDateRange(metadata, LocalDate.of(2018, 10, 2), LocalDate.of(2018, 10, 20)) shouldEqual false
  }

  it should "check date ranges correctly with multiple intervals" in {
    val startDate = LocalDate.of(2018, 9, 1)
    val endDate = LocalDate.of(2018, 9, 30)
    val startDate2 = LocalDate.of(2018, 10, 1)
    val endDate2 = LocalDate.of(2018, 10, 31)

    val startTime = Timestamp.valueOf(startDate.atStartOfDay)
    val endTime = Timestamp.valueOf(endDate.atStartOfDay)
    val startTime2 = Timestamp.valueOf(startDate2.atStartOfDay)
    val endTime2 = Timestamp.valueOf(endDate2.atStartOfDay)

    val metadata = Array(PagecountMetadata(startTime, endTime), PagecountMetadata(startTime2, endTime2))
    checkDateRange(metadata, LocalDate.of(2018, 9, 2), LocalDate.of(2018, 10, 4)) shouldEqual true
    checkDateRange(metadata, LocalDate.of(2018, 9, 1), LocalDate.of(2018, 10, 31)) shouldEqual true
    checkDateRange(metadata, LocalDate.of(2018, 9, 20), LocalDate.of(2018, 11, 2)) shouldEqual false
    checkDateRange(metadata, LocalDate.of(2018, 11, 2), LocalDate.of(2018, 11, 20)) shouldEqual false
  }
}

