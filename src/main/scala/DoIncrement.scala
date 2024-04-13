import org.apache.spark.sql._
import Util._
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._

case class BuyerData(id: String, firstName: String, lastName: String, email: String, gender: String)

case class CarData(id: String, carModel: String, carModelYear: Integer, carMaker: String, country: String, city: String)

object DoIncrement {

  def apply(sourcePath: String, targetPath: String): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("do-increment")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val BuyerData = readCSV[BuyerData](s"$sourcePath/buyer/*.csv".replace("//", "/"))
    val carData = readCSV[CarData](s"$sourcePath/car/*.csv".replace("//", "/"))

    BuyerData
      .join(carData, Seq("id"), "left")
      .withColumn("loadedDate", current_date())
      .repartition(3)
      .write
      .mode("append")
      .partitionBy("loadedDate")
      .parquet(s"$targetPath/cars_and_buyers/".replace("//", "/"))

    //Clean all files after reading
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val theSourcePathList = List(
                                 new Path(s"$sourcePath/buyer"),
                                 new Path(s"$sourcePath/car")
                                )
    for (p <- theSourcePathList) {
      if (fs.exists(p))
        fs.delete(p, true)
    }
  }

}
