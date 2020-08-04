import java.io.File

import com.da.insurance.datalake.OvernightSubmissionsProcessingService
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

import scala.reflect.io.Directory

class OvernightSubmissionsProcessingServiceTest extends FunSuite {
  val pathToDestination = "C:\\tmp\\hudi\\"
  var isLocalRun: Boolean = true

  test("OvernightSubmissionsProcessingService.processInitialOvernightData") {
    // arrange
    // clean the dest dir
    val directory = new Directory(new File(pathToDestination))
    directory.deleteRecursively()
    val pathToSource = getClass.getResource("/overnight-submissions/overnight_submissions_new_business_sub_id_1001_cnt_10_10d_from_now_1.csv")
    val isInitialRun: Boolean = true
    val args: Array[String] = Array(pathToSource.getPath, pathToDestination, isInitialRun.toString, isLocalRun.toString)

    // act
    val processor = new OvernightSubmissionsProcessingService()
    processor.processOvernightData(args)

    // assert
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.hive.convertMetastoreParquet", "false")
        .master("local[*]")
        .config("spark.driver.host", "localhost")
        .getOrCreate()

      val hudiDf = spark
        .read
        .format("org.apache.hudi")
        .load(pathToDestination + "/*/*/*/*")

      val subId = 1001
      val subDf = hudiDf
        .where("submission_id = " + subId)

      val row = subDf
        .select("submission_status", "underwriter")
        .first()

      val status = row(0)
      val underwriter = row(1)

      println(s"Sub status = $status, Underwriter = $underwriter")
      assert(status === "open")
      assert(underwriter === "")
    }
    finally {
      if (spark != null) {
        spark.stop()
      }

      if (directory != null) {
        directory.deleteRecursively()
      }
    }
  }
}
