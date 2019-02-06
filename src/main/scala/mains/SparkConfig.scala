package mains

import org.apache.spark.sql.SparkSession

/**
  *
  * @param appName
  */
case class SparkConfig(appName: String) {
  def session(): SparkSession = SparkSession.builder()
    .appName(s"SparkSession for $appName")
    .master("local[2]")
    .config("spark.logConf", "true")
    .getOrCreate()
}
