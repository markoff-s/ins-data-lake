package com.da.insurance.datalake

import org.apache.hudi.DataSourceReadOptions
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.util.Try

object HudiIncrementalViewer {

  val pathToSchema = "submissions_overnight_schema.json"

  // hudi config
  val hudiDbName = "ins_data_lake_transformed"
  val hudiTableName = "hudi_submissions"
  val hudiTableRecordKey = "submission_id"
  val hudiTablePartitionColumn = "submitted_on"
  // if 2 records have same key, the one with bigger precombine key wins
  val hudiTablePrecombineKey = "timestamp"
  var hudiSyncToHive = true

  def main(args: Array[String]): Unit = {

    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)

    var isLocalRun: Boolean = false
    if (args.length > 2) isLocalRun = Try(args(2).toBoolean).getOrElse(false)
    hudiSyncToHive = !isLocalRun

    var spark: SparkSession = null
    try {
      var sessionBuilder = SparkSession
        .builder()
        .appName("HudiIncrementalViewer")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.hive.convertMetastoreParquet", "false")

      if (isLocalRun)
        sessionBuilder = sessionBuilder
          .master("local[*]")
          .config("spark.driver.host", "localhost")

      spark = sessionBuilder.getOrCreate()

      val hoodieIncViewDF = spark.read
        .format("org.apache.hudi")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY, DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "20200617080000") //20200617083404
        .load(pathToSource); // For incremental view, pass in the root/base path of dataset

      hoodieIncViewDF
        .coalesce(1)
        .write
        .mode(SaveMode.Append)
        .format("csv")
        .option("header", "true")

        .save(pathToDestination)

    }
    finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
