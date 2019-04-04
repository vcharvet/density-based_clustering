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
    .config("spark.driver.cores", "1")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.cores", "1")
    .config("spark.executor.memory", "16g")
    .config("spark.executor.cores.max", "1")
    .config("spark.default.parallelism", "200")
    .config("spark.logConf", "true")
    .getOrCreate()
}
