import org.apache.spark.sql.SparkSession

import java.util.Properties
object DoExportToExternalStorage {

  def apply(sourcePath: String, targetHostPort: String, targetDBAndTable: String, targetOrderBy: String): Unit = {
    implicit val spark: SparkSession = SparkSession
      .builder()
      .appName("do-export-to-external-storage")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()

    println(
      s"""Exporting to external storage. Parameters:
        |sourcePath - $sourcePath,
        |targetHostPort - $targetHostPort,
        |targetDBAndTable - $targetDBAndTable.
        |""".stripMargin)

    val df = spark.read.parquet(s"$sourcePath")
    println(s"Loaded dataset to export")
    df.printSchema()

    //Where to export
    val targetDB = targetDBAndTable.trim.split('.')(0)
    val targetTableName = targetDBAndTable.trim.split('.')(1)
    val targetUser = spark.conf.get("spark.jdbc.password").trim.split('#')(0)
    val targetPassword = spark.conf.get("spark.jdbc.password").trim.split('#')(1)

    val jdbcUrl = s"jdbc:clickhouse://$targetHostPort/$targetDB"
    val ckProperties = new Properties()
    ckProperties.put("user", targetUser)
    ckProperties.put("password", targetPassword)

    df
      .distinct()
      .write.mode("Overwrite")
      .option("driver", "ru.yandex.clickhouse.ClickHouseDriver")
      .option("createTableOptions", s"ENGINE=MergeTree ORDER BY ($targetOrderBy)")
      .jdbc(jdbcUrl, table = targetTableName, ckProperties)

    println(s"Dataset from $sourcePath successfully exported to $targetDBAndTable." )
  }

}
