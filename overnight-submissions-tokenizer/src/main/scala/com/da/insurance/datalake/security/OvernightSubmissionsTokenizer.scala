package com.da.insurance.datalake.security

import org.apache.spark.sql.functions.{col, expr, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

object OvernightSubmissionsTokenizer {

  def main(args: Array[String]): Unit = {

    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)
    val pathToTokenMap: String = args(2)

    var isLocalRun: Boolean = false
    if (args.length > 3) isLocalRun = Try(args(3).toBoolean).getOrElse(false)

    var spark: SparkSession = null
    try {
      var sessionBuilder = SparkSession
        .builder()
        .appName("OvernightSubmissionsTokenizer")

      if (isLocalRun)
        sessionBuilder = sessionBuilder
          .master("local[*]")
          .config("spark.driver.host", "localhost")

      spark = sessionBuilder.getOrCreate()

      val getMaskedValueUdf = udf(getMaskedValue(_: String, _: Int): String)
      val sourceData = spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(pathToSource)
        .withColumn("masked_zip_4_chars", getMaskedValueUdf(col("zip"), lit(4)))
        .withColumn("masked_zip_2_chars", getMaskedValueUdf(col("zip"), lit(2)))
        .withColumn("zip_token", expr("uuid()"))

      Console.printf("# of partitions in source data = %d", sourceData.rdd.partitions.size)
      Console.println()

      // save the token map
      sourceData
        .select("zip_token", "zip")
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite) // overwrite for now - it's easier to test
        .save(pathToTokenMap)

      // save masked source data w/o PII data
      sourceData
        .drop("zip")
        .coalesce(1) // save as one file for now
        //        .repartition(3)
        .write
        .format("csv")
        .option("header", "true")
        .mode(SaveMode.Overwrite) // overwrite for now - it's easier to test
        .save(pathToDestination)

    }
    finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  private def getMaskedValue(text: String, numCharsToKeep: Int): String = {
    if (text == null || text.trim.isEmpty)
      return ""

    val textLength = text.length
    var charsToKeep = text
    if (textLength > numCharsToKeep) {
      charsToKeep = text.substring(textLength - numCharsToKeep, textLength)
    }

    "*" * (5 - numCharsToKeep) + charsToKeep
  }
}
