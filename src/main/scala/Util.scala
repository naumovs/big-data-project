import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._

import scala.reflect.runtime.universe.TypeTag
object Util {
  def schemaOf[T: TypeTag]: StructType = {
    ScalaReflection
      .schemaFor[T]
      .dataType
      .asInstanceOf[StructType]
  }

  def readCSV[T: Encoder : TypeTag](filePath: String)(implicit spark: SparkSession): Dataset[T] = {
    spark.read
      .option("header", "true")
      .option("dateFormat", "yyyy-MM-dd HH:mm:ss.SSS")
      .schema(schemaOf[T])
      .csv(filePath)
      .as[T]
  }

  implicit class ExtendedDataFrame(df: DataFrame) {
    def to[T <: Product : TypeTag]: Dataset[T] = {
      import df.sparkSession.implicits._
      import org.apache.spark.sql.functions.col
      df.select(Encoders.product[T].schema.map(f => col(f.name)): _*).as[T]
    }
  }

}
