/*package ch.epfl.lts2.wikipedia

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DumpParserTest extends AnyFlatSpec with Matchers {

  "splitSqlInsertLine" should "correctly extract the values part of an SQL insert line" in {
    val line = "INSERT INTO `categorylinks` VALUES (10,'Redirects_from_moves','*..2NN:,@2.FBHRP:D6ÜœÜ','2024-04-15 14:38:05','','uca-default-u-kn','page')"
    val expected = "(10,'Redirects_from_moves','*..2NN:,@2.FBHRP:D6ÜœÜ','2024-04-15 14:38:05','','uca-default-u-kn','page')"
    val result = new DumpParser().splitSqlInsertLine(line)
    result shouldEqual expected
  }
}

object DumpParserTest {
  def main(args: Array[String]): Unit = {
    (new DumpParserTest).execute()
  }
}
*/

