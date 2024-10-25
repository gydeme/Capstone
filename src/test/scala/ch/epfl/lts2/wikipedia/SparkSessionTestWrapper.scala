package ch.epfl.lts2.wikipedia

import org.apache.spark.sql.SparkSession

trait SparkSessionTestWrapper {
  lazy val spark: SparkSession = {
    SparkSession.builder()
      .master("local")
      .appName("spark test runner")
      .config("spark.driver.host", "localhost")
      .getOrCreate()
  }
}

