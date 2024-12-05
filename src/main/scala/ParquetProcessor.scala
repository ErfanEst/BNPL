import org.apache.spark.sql.{SparkSession, SaveMode}

object ParquetProcessor {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Parquet Processor")
      .master("local[*]")
      .getOrCreate()

    val inputPath = "/home/erfan/parquet_data/DEFAULT.BNPL_PACKAGE_PURCHASE_140304_1.parquet"
    val outputPath = "/home/erfan/parquet_test/Raw_data/package_purchase.parquet"

    val inputData = spark.read.parquet(inputPath)
    println(inputData.columns.mkString("Array(", ", ", ")"))


    val first50Rows = inputData.limit(50)

    first50Rows.write
      .mode(SaveMode.Overwrite)
      .parquet(outputPath)

    spark.stop()
  }
}
