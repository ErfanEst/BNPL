package extract

import core.Core.appConfig
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, regexp_replace, to_date, to_timestamp, unix_timestamp}
import task.FeatureMaker.index
import utils.Utils.CommonColumns.{month_index, nidHash}

object DataReader {

  Logger.getLogger("org").setLevel(Level.WARN)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)

  lazy val spark: SparkSession = SparkSession.builder
    .appName(appConfig.getString("spark.appName"))
    .master("local[*]") // Use 10 cores to match your system capacity while leaving 2 cores free for the OS
    .config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.executor.memory", "16g") // 8GB memory for each executor
    .config("spark.driver.memory", "16g") // 8GB memory for the driver
    .config("spark.executor.memoryOverhead", "2g") // 2GB overhead for executor memory
    .config("spark.sql.inMemoryColumnarStorage.compressed", "true") // Keep compressed in-memory storage
    .config("spark.sql.shuffle.compress", "true") // Enable shuffle compression
    .config("spark.sql.sort.spill.compress", "true") // Enable spill compression
    .config("spark.sql.shuffle.partitions", "400") // Adjust partitions for shuffles
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") // Disable broadcast joins to avoid OOM
    .config("spark.local.dir", "/home/erfan/rdd_test") // Temporary directory for shuffle and spill files
    .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC") // Use G1GC for better garbage collection
    .config("spark.driver.extraJavaOptions", "-XX:+UseG1GC") // Use G1GC for driver
    .config("spark.executor.extraJavaOptions=-Dlog4j.rootCategory=WARN","console")
    .config("spark.driver.extraJavaOptions=-Dlog4j.rootCategory=WARN","console")
    .getOrCreate


  val selectReader: (String, Map[String, List[String]], Int) => DataFrame = {
    (name: String, featureTableMap: Map[String, List[String]], index: Int) =>
      val tableName = featureTableMap(name).head
      if (readTable.isDefinedAt(tableName)) readTable(tableName) else throw new IllegalArgumentException(s"Unknown table: $tableName")
  }

  def selectCols(dataFrame: DataFrame)(cols: Seq[String]): DataFrame = {
    val df = dataFrame
      .select(cols.map(c => col(c)): _*)
    df.show(10)
    df
  }


  val readTable: PartialFunction[String, DataFrame] = {
    case x if List("package").contains(x) => readPackage(x, index)
    case x if List("cdr").contains(x) => readCDR(x, index)
    case x if List("user_info").contains(x) => readUserInfo(x, index)
    case x if List("package_purchase").contains(x) => readPackagePurchase(x, index)
  }

  val readUserInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    println("in the readUserInfo")
    fileType match {
      case "user_info" =>
        val user = spark.read.parquet(appConfig.getString("Path.UserInfo"))
          .filter(col(nidHash).isNotNull)
          .repartition(300)
          .withColumn("month_index", lit(index))
        user
    }
  }

  val readPackage: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package" =>
//        println(appConfig.getString("inputPath") + "/" + appConfig.getString("addresses.Package"))
        val pkg = spark.read.parquet(appConfig.getString("Path.Package"))
          .filter(col(nidHash).isNotNull)
          .repartition(300)
          .withColumn("month_index", lit(index))
        val preProcessedDf = pkg
          .withColumn("de_a_date", unix_timestamp(to_timestamp(col("deactivation_date"), "yyyyMMdd HH:mm:ss")))
          .withColumn("a_date", unix_timestamp(to_timestamp(col("activation_date"), "yyyyMMdd HH:mm:ss")))

        preProcessedDf
    }

  }

  val readCDR: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    println("in the CDR reading table")
    fileType match {
      case "cdr" =>
        spark.read.parquet(appConfig.getString("Path.CDR"))
          .filter(col(nidHash).isNotNull)
          .repartition(300)
          .withColumn("month_index", lit(index))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .drop("date_key")
    }
  }

  val readPackagePurchase: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    println("in the readPackagePurchase")
    fileType match {
      case "package_purchase" =>
        val packagePurchase = spark.read.parquet(appConfig.getString("Path.PackagePurchase"))
          .withColumn("month_index", lit(index))
        packagePurchase
    }
  }

  def setTimeRange(dataFrame: DataFrame)(indices: Seq[Int], range: Int = 0): DataFrame = {
    dataFrame
      .where(col(month_index) > indices.min - range).where(col(month_index) <= indices.max)
  }
}
