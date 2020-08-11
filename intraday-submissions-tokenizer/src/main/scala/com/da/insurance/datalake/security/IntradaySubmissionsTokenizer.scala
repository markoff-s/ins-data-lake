package com.da.insurance.datalake.security

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

object IntradaySubmissionsTokenizer {

  def main(args: Array[String]): Unit = {
    val (pathToSource: String, pathToDestination: String, pathToTokenMap: String) = ValidateInputArgs(args)

    var isLocalRun: Boolean = false
    if (args.length > 3) isLocalRun = Try(args(3).toBoolean).getOrElse(false)

    var spark: SparkSession = null
    try {

      var sessionBuilder = SparkSession
        .builder()
        .appName("IntradaySubmissionsProcessor")

      if (isLocalRun)
        sessionBuilder = sessionBuilder
          .master("local[6]")
          .config("spark.driver.host", "localhost")

      spark = sessionBuilder.getOrCreate()

      val sourceData = spark
        .read
        .format("xml")
        .option("rootTag", "Submission")
        .option("rowTag", "Submission")
        .load(pathToSource)
        .withColumn("SubmissionDate", regexp_replace(to_date(col("SubmissionDate"), "MM/dd/yyyy"), "-", "/"))

      /*sourceData.printSchema()
      sourceData.show()*/

      val getMaskedValueUdf = udf(getMaskedValue(_: String, _: Int): String)
      var sourceDataWithRenamedColumns = sourceData
        .withColumnRenamed("SubmissionID", "submission_id")
        .withColumnRenamed("SubmissionStatus", "submission_status")
        .withColumnRenamed("SubmissionDate", "submitted_on")
        .withColumnRenamed("City", "city")
        .withColumnRenamed("Company", "organization")
        .withColumnRenamed("State", "state")
        .withColumnRenamed("Street", "street")
        .withColumnRenamed("Zip", "zip")
        .withColumnRenamed("Underwriter", "underwriter")
        .withColumn("masked_zip_4_chars", getMaskedValueUdf(col("zip"), lit(4)))
        .withColumn("masked_zip_2_chars", getMaskedValueUdf(col("zip"), lit(2)))
        .withColumn("zip_token", expr("uuid()"))

      def renameColumnOrAddNew(origName: String, newName: String): Unit = {
        if (sourceData.columns.contains(origName)) {
          sourceDataWithRenamedColumns = sourceDataWithRenamedColumns
            .withColumnRenamed(origName, newName)
        }
        else {
          sourceDataWithRenamedColumns = sourceDataWithRenamedColumns
            .withColumn(newName, lit(null).cast(org.apache.spark.sql.types.IntegerType))
        }
      }

      renameColumnOrAddNew("YearBuilt", "year_built")
      renameColumnOrAddNew("BuildingValue", "building_value")
      /*sourceDataWithRenamedColumns.printSchema()
      sourceDataWithRenamedColumns.show()*/

      // save the token map
      sourceDataWithRenamedColumns
        .select("zip_token", "zip")
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .save(pathToTokenMap)

      // save masked source data w/o PII data
      sourceDataWithRenamedColumns
        //        .drop("zip")
        .select("submission_id", "submitted_on", "submission_status",
          "underwriter", "organization", "street", "city", "state", "year_built", "building_value",
          "masked_zip_4_chars", "masked_zip_2_chars", "zip_token")
        .write
        .format("csv")
        .option("header", "true")
        .option("emptyValue", "")
        .mode(SaveMode.Append)
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

  private def ValidateInputArgs(args: Array[String]) = {
    val pathToSource: String = args(0)
    if (pathToSource == null || pathToSource.isEmpty)
      throw new IllegalArgumentException("Path to source is null or empty")

    val pathToDestination: String = args(1)
    if (pathToDestination == null || pathToDestination.isEmpty)
      throw new IllegalArgumentException("Path to destination is null or empty")

    val pathToTokenMap: String = args(2)
    if (pathToTokenMap == null || pathToTokenMap.isEmpty)
      throw new IllegalArgumentException("Path to token map is null or empty")

    (pathToSource, pathToDestination, pathToTokenMap)
  }
}
