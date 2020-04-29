package com.da.insurance.datalake

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SubmissionsReportGenerator {

  val hudiTableName = "hudi_submissions"

  def main(args: Array[String]): Unit = {

    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)

    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .appName("SubmissionsReportGenerator")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()

      val submissionsDf = spark
        .read
        .format("org.apache.hudi")
        .load(s"$pathToSource/*/*/*/*")
        .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path",
          "_hoodie_file_name")
        .cache()

      // write all submissions report
      submissionsDf
        .write
        .format("parquet")
        .mode("overwrite")
        .save(s"$pathToDestination/submissions_all/")

      // write submissions-by-state report
      submissionsDf
        .filter(col("state").isNotNull)
        .groupBy(col("state"), col("city"), col("zip"))
        .agg(
          count("submission_id").as("submissions_count")
        )
        .orderBy(col("state"), col("city"), col("zip"))
        .write
        .format("parquet")
        .mode("overwrite")
        .save(s"$pathToDestination/submissions_by_state_city_zip/")

      // write all submissions report - need to enable hive support
      /*spark
        .sql(s"select * from $hudiTableName")
        .drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key", "_hoodie_partition_path",
          "_hoodie_file_name")
        .write
        .format("parquet")
        .mode("overwrite")
        .save(s"$pathToDestination/submissions_all/")*/


    }
    finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }
}
