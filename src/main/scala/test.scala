import org.apache.spark.sql.{SparkSession, SaveMode}

object ParquetProcessor {
  def main(args: Array[String]): Unit = {
    // ایجاد یک SparkSession
    val spark = SparkSession.builder()
      .appName("Parquet Processor")
      .master("local[*]") // برای اجرا در لوکال
      .getOrCreate()

    // مسیر فایل ورودی و خروجی
    val inputPath = "/home/erfan/parquet_data/DEFAULT.BNPL_AAT_LABS_CUSTOMER_INFO_BALANCE_SITEID_20240609.parquet"
    val outputPath = "/home/erfan/parquet_test/userInfo.parquet"

    // خواندن فایل Parquet
    val inputData = spark.read.parquet(inputPath)
    println(inputData.columns.mkString("Array(", ", ", ")"))

    // گرفتن 20 ردیف اول
//    val first20Rows = inputData.limit(20)
//
//    // ذخیره داده‌ها در مسیر جدید
//    first20Rows.write
//      .mode(SaveMode.Overwrite) // برای بازنویسی فایل در صورت وجود
//      .parquet(outputPath)

    // خاتمه SparkSession
    spark.stop()
  }
}
