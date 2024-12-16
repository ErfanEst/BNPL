package extract

import core.Core.SourceCol.Arpu.{flagSimTierMode, genderMode, siteTypeMode}
import core.Core.{appConfig, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType
import task.FeatureMaker.index
import utils.Utils.CommonColumns.{month_index, nidHash}
import utils.Utils.monthIndexOf

object DataReader {
  val selectReader: (String, Map[String, List[String]]) => DataFrame =
    (name: String, featureTableMap: Map[String, List[String]]) => readTable(featureTableMap(name).head)

  def selectCols(dataFrame: DataFrame)(cols: Seq[String]): DataFrame = {
    val df = dataFrame
      .select(cols.map(c => col(c)): _*)
    df
  }

  private val readTable: PartialFunction[String, DataFrame] = {
    case x if List("package").contains(x) => readPackage(x, index)
    case x if List("cdr").contains(x) => readCDR(x, index)
    case x if List("user_info").contains(x) => readUserInfo(x, index)
    case x if List("package_purchase").contains(x) => readPackagePurchase(x, index)
    case x if List("handset_price").contains(x) => readHandSetPrice(x, index)
    case x if List("arpu").contains(x) => readArpu(x, index)
  }

  private val readUserInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
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

  private val readPackage: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package" =>
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

  private val readCDR: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "cdr" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        spark.read.parquet(appConfig.getString("Path.CDR"))
          .filter(col(nidHash).isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .repartition(300)
          .drop("date_key")
    }
  }

  private val readPackagePurchase: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    println("in the readPackagePurchase")
    fileType match {
      case "package_purchase" =>
        val packagePurchase = spark.read.parquet(appConfig.getString("Path.PackagePurchase"))
          .withColumn("month_index", lit(index))
        packagePurchase
    }
  }

  private val readHandSetPrice: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    println("in the readHandSetPrice")
    fileType match {
      case "handset_price" =>
        var handsetPrice = spark.read.parquet(appConfig.getString("Path.HandsetPrice"))
          .withColumn("month_index", lit(index))
        handsetPrice = handsetPrice.dropDuplicates()
        handsetPrice = handsetPrice.filter(col("handset_brand").isNotNull)
        handsetPrice = handsetPrice.filter(col("cnt_of_days").isNotNull)
        handsetPrice = handsetPrice.withColumn("handset_brand_2", expr("regexp_replace(handset_brand, r'^(?!.*(SAMSUNG|XIAOMI|HUAWEI|APPLE)).*$', 'Other')"))
        handsetPrice
    }
  }

  private val readArpu: (String, Int) => DataFrame = { (fileType: String, index: Int) =>

    fileType match {
      case "arpu" =>
        val arpu = spark.read.parquet(appConfig.getString("Path.Arpu"))
          .repartition(300)
        val arpuMsisdn: DataFrame = arpu
          .groupBy("fake_msisdn", "fake_ic_number")
          .agg(
            when(last("gender") === "F", 1).otherwise(0).alias("gender"),
            max("age").alias("age"),
            last("site_type").alias("site_type"),
            max("res_com_score").alias("res_com_score"),
            max("voice_revenue").alias("voice_revenue"),
            last("flag_sim_tier").alias("flag_sim_tier"),
            max("gprs_revenue").alias("gprs_revenue"),
            max("sms_revenue").alias("sms_revenue"),
            max("subscription_revenue").alias("subscription_revenue")
          )

        calculateCustomerLevelMetrics(arpuMsisdn)

        println("in the data reader......")
        arpuMsisdn.show(30, truncate = false)

        arpuMsisdn.withColumn("month_index", lit(index))
    }
  }


  def setTimeRange(dataFrame: DataFrame)(indices: Seq[Int], range: Int = 0): DataFrame = {
    dataFrame
      .where(col(month_index) > indices.min - range).where(col(month_index) <= indices.max)
  }

  private def calculateCustomerLevelMetrics(arpuMsisdn: DataFrame): Unit = {

    // Step 1: Calculate aggregations
    val dfCount = arpuMsisdn.groupBy("fake_ic_number", "site_type").count()

    // Step 2: Pivot the DataFrame, creating one column for each `site_type`
    val dfPivot = dfCount.groupBy("fake_ic_number")
      .pivot("site_type")
      .sum("count")
      .na.fill(0)

    // Step 3: Rename columns to reflect the site_type (optional but recommended)
    val distinctSiteTypes = arpuMsisdn.select("site_type")
      .distinct()
      .rdd
      .map(row => row.getString(0))
      .filter(_ != null)
      .collect()

    siteTypeMode = dfPivot.select(
      col("fake_ic_number") +:
        distinctSiteTypes.map(siteType => col(s"`$siteType`").alias(s"site_type_$siteType")): _*
    )

    flagSimTierMode = arpuMsisdn
      .filter(col("flag_sim_tier").isNotNull)
      .groupBy("fake_ic_number", "flag_sim_tier")
      .count()
      .withColumn("rank", row_number().over(Window.partitionBy("fake_ic_number").orderBy(desc("count"))))
      .filter(col("rank") === 1)
      .select(
        col("fake_ic_number"),
        col("flag_sim_tier").alias("flag_sim_tier_mode").cast(IntegerType)
      )

    genderMode = arpuMsisdn
      .filter(col("gender").isNotNull)
      .groupBy("fake_ic_number", "gender")
      .count()
      .withColumn("rank", row_number().over(Window.partitionBy("fake_ic_number").orderBy(desc("count"))))
      .filter(col("rank") === 1)
      .select(
        col("fake_ic_number"),
        col("gender").alias("gender").cast(IntegerType)
      )
  }
}
