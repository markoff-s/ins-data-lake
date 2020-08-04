package com.da.insurance.datalake

object OvernightSubmissionsProcessor {
  def main(args: Array[String]): Unit = {
    val processor = new OvernightSubmissionsProcessingService()
    processor.processOvernightData(args)
  }
}


