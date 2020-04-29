package com.da.insurance.datalake

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions.{col, current_timestamp, regexp_replace, to_date, lit}
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

object OvernightSubmissionsProcessor {

  val pathToSchema = "submissions_overnight_schema.json"

  // hudi config
  val hudiDbName = "ins_data_lake_transformed"
  val hudiTableName = "hudi_submissions"
  val hudiTableRecordKey = "submission_id"
  val hudiTablePartitionColumn = "submitted_on"
  // if 2 records have same key, the one with bigger precombine key wins
  val hudiTablePrecombineKey = "timestamp"

  def main(args: Array[String]): Unit = {

    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)
    var isInitialRun: Boolean = false
    if (args.length > 2) isInitialRun = Try(args(2).toBoolean).getOrElse(false)

    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .appName("OvernightSubmissionsProcessor")
        .getOrCreate()

      val schemaFromJson = loadSchema(pathToSchema)

      val sourceData = spark
        .read
        .format("csv")
        .option("header", "true")
        .schema(schemaFromJson)
        .load(pathToSource)
        .withColumn(hudiTablePrecombineKey, current_timestamp().cast("long"))
        .withColumn(hudiTablePartitionColumn, regexp_replace(to_date(col("Submitted On"), "MM/dd/yy"), "-", "/"))
        .withColumn("underwriter", lit("").cast(StringType))
        .drop("Submitted On")
      //.cache()

      val cleanedSourceData = normalizeColumnNames(sourceData)

      saveToHudi(cleanedSourceData, pathToDestination, isInitialRun)

    }
    finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  private def loadSchema(pathToSchema: String) = {
    // somehow this is not loading on EMR... smth wrong with the path
    /*val url = ClassLoader.getSystemResource(pathToSchema)
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString*/

    val schemaSource = "{\"fields\":[{\"metadata\":{},\"name\":\"Submission ID\",\"nullable\":false,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Submission Status\",\"nullable\":false,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Submitted On\",\"nullable\":false,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Underwriter\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Company\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Street\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"City\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"State\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Zip\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Website\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Years at Location\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Principal Name\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Years in Business\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Nature of Operation\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Previous Carrier\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Expiry Date\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Expiring Premium\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Building Owned\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Area Occupied\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Stories\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Year Built\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Age\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Class of Construction\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Wall\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Roof\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Floor\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Roof Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Wiring Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Plumbing Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Heating Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Occupied Stories\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Adjacent North\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent South\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent East\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent West\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Fire Protection\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Fire Alarm\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Sprinklered\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Burglar Alarm\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Deadbolt Locks\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Other Protection\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Safe\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Safe Type\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Average Cash\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Maximum Cash\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Mortgagee\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Building Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Equipment Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Improvements Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Office Contents\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"EDP Equipment\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"EDP Data Media\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Laptops Projectors\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Customer Goods\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Others Property\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Stock Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Gross Earnings\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Profits\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Transit\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Employee Dishonesty\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Money Orders Securities\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Flood\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Earthquake\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Boiler\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Number of Boilers\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Boiler Maintenance Contract\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Air Conditioning\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Central Air Conditioning\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Tons\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Air Conditioning Maintenance Contract\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Automatic Backup Power\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Voltage Surge Suppression\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim Date\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim Description\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim Amount\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Signed Name\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Title Position\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"

    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }

  private def normalizeColumnNames(sourceData: DataFrame) = {
    // prepare columns for parquet
    // Column headers: lower case + remove spaces and the following characters: ,;{}()=
    var newColumns = scala.collection.mutable.ArrayBuffer.empty[String]
    val problematic_chars = List(",", ";", "{", "}", "(", ")", "=")
    for (column <- sourceData.columns) {
      var newColName = column.toLowerCase().replace(" ", "_")

      for (c <- problematic_chars) {
        newColName = newColName.replace(c, "")
      }

      newColumns += newColName
    }

    sourceData.toDF(newColumns: _*)
  }

  def saveToHudi(df: DataFrame, pathToDestination: String, isInitialRun: Boolean): Unit = {
    // prepare for save to hudi
    val hudiOptions = Map[String, String](
      HoodieWriteConfig.TABLE_NAME -> hudiTableName,

      //For this data set, we will configure it to use the COPY_ON_WRITE storage strategy.
      //You can also choose MERGE_ON_READ
      DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY -> "COPY_ON_WRITE",

      //These three options configure what Hudi should use as its record key,
      //partition column, and precombine key.
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> hudiTableRecordKey,
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> hudiTablePrecombineKey,
      DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> hudiTablePartitionColumn,

      //For this data set, we specify that we want to sync metadata with Hive.
      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "true",
      DataSourceWriteOptions.HIVE_DATABASE_OPT_KEY -> hudiDbName,
      DataSourceWriteOptions.HIVE_TABLE_OPT_KEY -> hudiTableName,
      DataSourceWriteOptions.HIVE_PARTITION_FIELDS_OPT_KEY -> hudiTablePartitionColumn
    )

    //write to Hudi
    var saveMode = SaveMode.Append
    var writeOperation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL
    if (isInitialRun) {
      saveMode = SaveMode.Overwrite
      writeOperation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL
    }

    df.write
      .format("org.apache.hudi")
      .options(hudiOptions)
      //Operation Key tells Hudi whether this is an Insert, Upsert, or Bulk Insert operation
      //INSERT_OPERATION_OPT_VAL //BULK_INSERT_OPERATION_OPT_VAL //UPSERT_OPERATION_OPT_VAL - read abt different options. e.g. can i use upsert all the time? even the first time?
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, writeOperation)
      .mode(saveMode)
      .save(pathToDestination + "/" + hudiTableName)
  }
}
