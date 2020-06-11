package com.da.insurance.datalake.security

import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

object OvernightSubmissionsTokenizer {

  //  val pathToSchema = "submissions_overnight_schema.json"

  def main(args: Array[String]): Unit = {

    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)

    var isLocalRun: Boolean = false
    if (args.length > 2) isLocalRun = Try(args(2).toBoolean).getOrElse(false)

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

      //      val schemaFromJson = loadSchema(pathToSchema)

      val getMaskedValueUdf = udf(getMaskedValue(_:String, _: Int):String)
      val sourceData = spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        //        .schema(schemaFromJson)
        .load(pathToSource)
//        .withColumn("underwriter", lit("").cast(StringType))
        .withColumn("masked_zip_4_chars", getMaskedValueUdf(col("zip"), lit(4)))
        .withColumn("masked_zip_2_chars", getMaskedValueUdf(col("zip"), lit(2)))

      //        .drop("submitted_on")
      //.cache()

      Console.printf("# of partitions in source data = %d", sourceData.rdd.partitions.size)
      Console.println()


      sourceData
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
