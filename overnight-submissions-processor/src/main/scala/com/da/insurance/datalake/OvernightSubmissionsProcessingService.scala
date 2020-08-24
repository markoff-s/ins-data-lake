package com.da.insurance.datalake

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.util.Try

class OvernightSubmissionsProcessingService {
  val pathToSchema = "submissions_overnight_schema.json"

  // hudi config
  val hudiDbName = "ins_data_lake_transformed"
  val hudiTableName = "hudi_submissions"
  val hudiTableRecordKey = "submission_id"
  val hudiTablePartitionColumn = "submitted_on"
  // if 2 records have same key, the one with bigger precombine key wins
  val hudiTablePrecombineKey = "timestamp"
//  var hudiSyncToHive = true

  def processOvernightData(args: Array[String]): Unit = {
    val pathToSource: String = args(0)
    val pathToDestination: String = args(1)
    var isInitialRun: Boolean = false
    if (args.length > 2) isInitialRun = Try(args(2).toBoolean).getOrElse(false)

    var isLocalRun: Boolean = false
    if (args.length > 3) isLocalRun = Try(args(3).toBoolean).getOrElse(false)
//    hudiSyncToHive = !isLocalRun

    var spark: SparkSession = null
    try {
      var sessionBuilder = SparkSession
        .builder()
        .appName("OvernightSubmissionsProcessor")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.hive.convertMetastoreParquet", "false")

      if (isLocalRun)
        sessionBuilder = sessionBuilder
          .master("local[3]")
          .config("spark.driver.host", "localhost")

      spark =  sessionBuilder.getOrCreate()

//      val schemaFromJson = loadSchema(pathToSchema)

      val sourceData = spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
//        .schema(schemaFromJson)
        .load(pathToSource)
        .withColumn(hudiTablePrecombineKey, current_timestamp().cast("long"))
        .withColumn(hudiTablePartitionColumn, regexp_replace(to_date(col("submitted_on"), "yyyy-MM-dd"), "-", "/"))
        .withColumn("underwriter", coalesce(col("underwriter"), lit("").cast(StringType)))
        
      // clean up col names in case there's something invalid
      val cleanedSourceData = normalizeColumnNames(sourceData)

      saveToHudi(cleanedSourceData, pathToDestination, isInitialRun)

    }
    finally {
      if (spark != null) {
        spark.stop()
      }
    }
  }

  // schema is easily inferred after switch to normalized source data
  /*private def loadSchema(pathToSchema: String) = {
    // somehow this is not loading on EMR... smth wrong with the path
    /*val url = ClassLoader.getSystemResource(pathToSchema)
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString*/

    val schemaSource = "{\"fields\":[{\"metadata\":{},\"name\":\"Submission_ID\",\"nullable\":false,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Submission_Status\",\"nullable\":false,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Submitted_On\",\"nullable\":false,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Underwriter\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Organization\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Street\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"City\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"State\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Zip\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Website\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Years_at_Location\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Principal_Name\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Years_in_Business\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Nature_of_Operation\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Previous_Carrier\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Expiry_Date\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Expiring_Premium\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Building_Owned\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Area_Occupied\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Stories\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Year_Built\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Age\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Class_of_Construction\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Wall\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Roof\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Floor\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Roof_Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Wiring_Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Plumbing_Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Heating_Updated\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Occupied_Stories\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Adjacent_North\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent_South\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent_East\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Adjacent_West\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Fire_Protection\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Fire_Alarm\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Sprinklered\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Burglar_Alarm\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Deadbolt_Locks\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Other_Protection\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Safe\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Safe_Type\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Average_Cash\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Maximum_Cash\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Mortgagee\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Building_Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Equipment_Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Improvements_Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Office_Contents\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"EDP_Equipment\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"EDP_Data_Media\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Laptops_Projectors\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Customer_Goods\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Others_Property\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Stock_Value\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Gross_Earnings\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Profits\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Transit\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Employee_Dishonesty\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Money_Orders_Securities\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Flood\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Earthquake\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Boiler\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Number_of_Boilers\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Boiler_Maintenance_Contract\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Air_Conditioning\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Central_Air_Conditioning\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Tons\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Air_Conditioning_Maintenance_Contract\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Automatic_Backup_Power\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Voltage_Surge_Suppression\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim_Date\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim_Description\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Claim_Amount\",\"nullable\":true,\"type\":\"integer\"},{\"metadata\":{},\"name\":\"Signed_Name\",\"nullable\":true,\"type\":\"string\"},{\"metadata\":{},\"name\":\"Title_Position\",\"nullable\":true,\"type\":\"string\"}],\"type\":\"struct\"}"

    DataType.fromJson(schemaSource).asInstanceOf[StructType]
  }*/

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

      DataSourceWriteOptions.HIVE_SYNC_ENABLED_OPT_KEY -> "false", // hudiSyncToHive.toString,
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
