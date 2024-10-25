package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

trait TestDataReal {
  val spark: SparkSession

  // Define the path to the small datasets
  val smallDataPath = "/home/gyde/Documents/bzsets/small/"

  // Function to read and collect data from bz2 files
  def readBz2File(path: String): String = {
    spark.read.textFile(path).collect().mkString("\n")
  }

  // Load the real data from bz2 files
  val sqlPage: String = readBz2File(s"${smallDataPath}enwiki-20240620-page.sql.bz2")
  val sqlPageLinks: String = readBz2File(s"${smallDataPath}enwiki-20240620-pagelinks.sql.bz2")
  val sqlRedirect: String = readBz2File(s"${smallDataPath}enwiki-20240620-redirect.sql.bz2")
  val sqlCategory: String = readBz2File(s"${smallDataPath}enwiki-20240620-category.sql.bz2")
  val sqlCatLink: String = readBz2File(s"${smallDataPath}enwiki-20240620-categorylinks.sql.bz2")

  // Load page count data
  val pageCountLegacy: String = readBz2File(s"${smallDataPath}pagecount-legacy.bz2")
  val pageViews: String = readBz2File(s"${smallDataPath}pageviews.bz2")
}


