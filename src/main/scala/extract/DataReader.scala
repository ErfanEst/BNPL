package extract

import core.Core.SourceCol.Arpu.{averageAge, flagSimTierMode, genderMode, mostFrequentFlagSimTier, mostFrequentGender, siteTypeMode}
import core.Core.{appConfig, spark}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType
import task.FeatureMaker.index
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
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
    case x if List("customer_person_type_bank_info").contains(x) => readBankInfo(x, index)
    case x if List("recharge").contains(x) => readRecharge(x, index)
  }

  private val readUserInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "user_info" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val user = spark.read.parquet(appConfig.getString("Path.UserInfo"))
          .filter(col(bibID).isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .repartition(300)
          .drop("date_key")
        user
    }
  }

  private val readPackage: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val pkg = spark.read.parquet(appConfig.getString("Path.Package"))
          .filter(col(nidHash).isNotNull)
          .repartition(300)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
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
          .filter(col(bibID).isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .repartition(300)
          .drop("date_key")
    }
  }

  private val readPackagePurchase: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package_purchase" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val packagePurchase = spark.read.parquet(appConfig.getString("Path.PackagePurchase"))
          .filter(col("amount") > lit(0) && col("cnt") > lit(0))
          .withColumn("month_index", monthIndexOfUDF(col("date")))
          .drop("date_key")

        packagePurchase
    }
  }

  private val readHandSetPrice: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "handset_price" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        var handsetPrice = spark.read.parquet(appConfig.getString("Path.HandsetPrice"))
          .withColumn("handset_brand_array", array("handset_brand"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")

        val cvm: CountVectorizerModel = new CountVectorizerModel(Array("SAMSUNG", "XIAOMI", "HUAWEI", "APPLE", "Other"))
          .setInputCol("handset_brand_array")
          .setOutputCol("handsetVec")

        handsetPrice = cvm.transform(handsetPrice)

        handsetPrice = handsetPrice.dropDuplicates()
        handsetPrice = handsetPrice.filter(col("handset_brand").isNotNull)
        handsetPrice = handsetPrice.filter(col("cnt_of_days").isNotNull)

        handsetPrice.show(100, truncate = false)

        handsetPrice
    }
  }

  private val readArpu: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "arpu" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val msisdn_metrics = spark.read.parquet(appConfig.getString("Path.Arpu"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("month_id")
          .drop("date_key")
          .repartition(300)
          .groupBy("fake_msisdn", "fake_ic_number", month_index)
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
          ).na.fill(Map(
            "voice_revenue" -> 0,
            "gprs_revenue" -> 0,
            "sms_revenue" -> 0,
            "subscription_revenue" -> 0
          ))
        calculateCustomerLevelMetrics(msisdn_metrics)

        msisdn_metrics
    }
  }

  private val readBankInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "customer_person_type_bank_info" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val user = spark.read.parquet(appConfig.getString("Path.BankInfo"))
          .filter(col("fake_ic_number").isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .repartition(300)
        user
    }
  }

  private val readRecharge: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "recharge" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val rech = spark.read.parquet(appConfig.getString("Path.Recharge"))
          .filter(col(bibID).isNotNull)
          .withColumn("recharge_dt", to_timestamp(col("recharge_dt"), "yyyyMMdd' 'HH:mm:ss"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop(col("date_key"))
          .repartition(300)
        rech
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

    dfPivot.printSchema()

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

    flagSimTierMode.printSchema()

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
//    val arpuCustomer = arpuMsisdn
//      .groupBy("fake_ic_number")
//      .agg(
//        // last("gender").alias("gender"), // Uncomment this line if needed, but `last` in Spark may require specific parameters.
//        max("age").alias("age"),
//        avg("res_com_score").alias("avg_res_com_score"),
//        avg("voice_revenue").alias("avg_voice_revenue"),
//        avg("gprs_revenue").alias("avg_gprs_revenue"),
//        avg("sms_revenue").alias("avg_sms_revenue"),
//        avg("subscription_revenue").alias("avg_subscription_revenue"),
//        count("fake_msisdn").alias("count_active_fake_msisdn")
//      )
//
//    val arpuCustomerJoint = arpuCustomer
//      .join(siteTypeMode, Seq("fake_ic_number"), "left")
//      .join(flagSimTierMode, Seq("fake_ic_number"), "left")
//      .join(genderMode, Seq("fake_ic_number"), "left")
//
//    arpuCustomerJoint.filter(col("count_active_fake_msisdn") < 100)
//
//    averageAge = arpuCustomerJoint.select(avg("age")).first().getDouble(0)
//    mostFrequentGender = 0
//    mostFrequentFlagSimTier = arpuCustomerJoint
//      .filter(col("flag_sim_tier_mode").isNotNull)
//      .groupBy("flag_sim_tier_mode")
//      .count()
//      .orderBy(desc("count"))
//      .first()
//      .get(0)
//
//
//    arpuCustomerJoint
  }

}
