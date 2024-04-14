import Util._
import org.apache.hadoop.fs._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

case class CarsAndBuyers(
                         firstName: String, lastName: String, email: String, gender: String,
                         carModel: String, carModelYear: Integer, carMaker: String, country: String, city: String,
                         loadedDate: java.sql.Date
                       ) // Column "id" skipped
case class LoadedPartitions (loadedDate: java.sql.Date, dataset: String)
object DoDataMart {
  def apply(sourcePath: String, targetPath: String): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("do-data-mart")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Prepare dataset of already loaded partitions
    val conf = spark.sparkContext.hadoopConfiguration
    val fs = FileSystem.get(conf)
    val jobUtilDir = s"$targetPath/job_util/loaded_partitions"
    val exists = fs.exists(new Path(jobUtilDir))

    val loadedPartitions = if (exists) {
      val lp = readCSV[LoadedPartitions](jobUtilDir)
      lp
        .repartition(1)
        .write.format("com.databricks.spark.csv")
        .option("header", "true")
        .mode("Overwrite")
        .csv(s"${jobUtilDir}_tmp")
      readCSV[LoadedPartitions](s"${jobUtilDir}_tmp").cache()
    } else {
      spark.emptyDataset[LoadedPartitions].cache()
    }

    val carsAndBuyers = spark.read.parquet(sourcePath).to[CarsAndBuyers]

    // Cars by country (load only recently added partitions)
    val windowSpecAgg = Window.partitionBy('loadedDate, 'country)
    println("Running cars by country")
    val carsAndBuyersToExport = carsAndBuyers
      .withColumn("carModelYear_median",
        expr("percentile_approx(carModelYear, 0.5)").over(windowSpecAgg))
      .join(broadcast(loadedPartitions), Seq("loadedDate"),"left_anti")
      .groupBy('loadedDate, 'country)
      .agg(
        countDistinct('carModel).as("carModel_cnt"),
        max('carModelYear_median).as("carModelYear_median")
      )

    println("The schema of carsAndBuyersToExport:")
    carsAndBuyersToExport.printSchema()

    carsAndBuyersToExport
      .repartition(3)
      .write
      .mode("append")
      .partitionBy("loadedDate")
      .parquet(s"$targetPath/cars_by_country/".replace("//", "/"))

    // Buyers by car model


    //Save loadedPartitions into jobUtilDir
    carsAndBuyersToExport
      .withColumn("dataset", typedLit("cars_and_buyers"))
      .select('loadedDate, 'dataset)
      .union(loadedPartitions.select('loadedDate, 'dataset))
      .distinct()
      .repartition(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .mode("Overwrite")
      .csv(jobUtilDir)

    loadedPartitions.unpersist()

    //Clean tmp dir
    val p = new Path(s"${jobUtilDir}_tmp")
    if (fs.exists(p)) fs.delete(p, true)

    println("Successfully saved to file list of loaded partitions.")
  }
}
