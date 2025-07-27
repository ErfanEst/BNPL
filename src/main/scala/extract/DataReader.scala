package extract

import core.Core.SourceCol.Arpu.{averageAge, flagSimTierMode, genderMode, mostFrequentFlagSimTier, mostFrequentGender, siteTypeMode}
import core.Core.{appConfig, spark}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType
import task.FeatureMaker.index
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.arpuDetails.other_sites
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
    case x if List("domestic_travel").contains(x) => readDomestic(x, index)
    case x if List("user_info").contains(x) => readUserInfo(x, index)
    case x if List("package_purchase").contains(x) => readPackagePurchase(x, index)
    case x if List("package_purchase_extras").contains(x) => readPackagePurchase(x, index)
    case x if List("package_purchase_avgs").contains(x) => readPackagePurchase(x, index)
    case x if List("handset_price").contains(x) => readHandSetPrice(x, index)
    case x if List("handset_price_brands").contains(x) => readHandSetPrice(x, index)
    case x if List("arpu").contains(x) => readArpu(x, index)
    case x if List("arpu_changes").contains(x) => readArpuChanges(x, index)
    case x if List("customer_person_type_bank_info").contains(x) => readBankInfo(x, index)
    case x if List("recharge").contains(x) => readRecharge(x, index)
    case x if List("loan_assign").contains(x) => readLoanAssign(x, index)
    case x if List("loan_rec").contains(x) => readLoanRec(x, index)
    case x if List("post_paid").contains(x) => readPostPaid(x, index)
    case x if List("credit_management").contains(x) => readCreditManagement(x, index)

  }

  private val readCreditManagement: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "credit_management" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val creditRaw = spark.read.parquet(appConfig.getString("Path.CreditManagement"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .filter(col("credit_assigned_date") > "2024-12-16")
          .dropDuplicates()

        val maxMonthIndex = creditRaw.agg(max(month_index)).first().get(0)

        val creditProcessed = creditRaw
          .withColumn(month_index, lit(maxMonthIndex))
          .filter(col("loan_status") =!= 3)
          .withColumn(
            "days_delay_new",
            when(
                col("days_delayed").isNull,
                datediff(col("date"), col("installment_duedate"))
              ).otherwise(col("days_delayed"))
          ).withColumn(
            "days_after_duedate",
            datediff(col("date"), col("installment_duedate"))
          )

        val grouped = creditProcessed.groupBy("fake_msisdn", "installment_id").agg(max("days_delay_new").alias("days_delay_new"), max("days_delayed").alias("days_delayed"), max("days_after_duedate").alias("days_after_duedate"), max(month_index).alias("month_index"))

        val result = grouped
          .withColumn("debt_status_1",
          when(col("days_after_duedate") <= 31 && col("days_delay_new") > 0 && (col("days_delay_new") === col("days_delayed")), 4)
            .when(col("days_after_duedate") <= 31 && col("days_delay_new") > 0 && col("days_delayed").isNull, 2)
            .when(col("days_after_duedate") <= 31 && col("days_delay_new") <= 0 && (col("days_delay_new") === col("days_delayed")), 3)
            .when(col("days_after_duedate") <= 31 && col("days_delay_new") <= 0 && col("days_delayed").isNull, 1)
            .otherwise(-1)
        ).withColumn(
          "debt_status_2",
          when((col("days_after_duedate") <= 62) && col("days_delay_new") > 0 && (col("days_delay_new") === col("days_delayed")), 4)
            .when((col("days_after_duedate") <= 62) && col("days_delay_new") > 0 && col("days_delayed").isNull, 2)
            .when((col("days_after_duedate") <= 62) && col("days_delay_new") <= 0 && (col("days_delay_new") === col("days_delayed")), 3)
            .when((col("days_after_duedate") <= 62) && col("days_delay_new") <= 0 && col("days_delayed").isNull, 1)
            .otherwise(-1)
        ).withColumn(
            "terrible_debt_status",
            when(col("days_after_duedate") >= 60 && col("days_delayed").isNull, 1)
              .when(col("days_after_duedate") >= 60 && (col("days_after_duedate") - col("days_delayed") <= 31), 2)
              .otherwise(-1)
          )

        result
    }
  }

  private val readPostPaid: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "post_paid" =>

        val w = Window.partitionBy("fake_msisdn").orderBy(month_index)

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val postPaid = spark.read.parquet(appConfig.getString("Path.PostPaid"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
//          .na.fill(Map(
//            "credit_limit" -> 0.0,
//            "deposit_amt_n" -> 0.0
//          ))
          .dropDuplicates()
          .withColumn("row_number", row_number().over(w))

        val changeOwnerships = spark.read.parquet("/home/erfan/Desktop/Change_ownership_list/drop_list_16846_16847")
          .dropDuplicates("bib_id", "nid_hash")
          .select("bib_id")
          .distinct()

        val postPaidFiltered = postPaid
          .join(changeOwnerships,
            postPaid("fake_msisdn") === changeOwnerships("bib_id"),
            "left_anti")



        val afterCount = postPaidFiltered.count()
        println(s"After filtering (postPaidFiltered) count: $afterCount")
        val beforeCount = postPaid.count()
        println(s"Before filtering (postPaid) count: $beforeCount")
        println(s"Dropped rows: ${beforeCount - afterCount}")
        Thread.sleep(10000)

        postPaid.filter(col("fake_msisdn") === "018B5E24A97654D3029C4CE8DAC57364").show(false)
        postPaid.printSchema()
        Thread.sleep(10000)

        postPaid

    }
  }

  private val readDomestic: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "domestic_travel" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val domestic = spark.read.parquet(appConfig.getString("Path.DomesticTravel"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))


//          .drop("date_key")

        domestic
    }
  }

  /*
    private val readPackagePurchaseExtras: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
      fileType match {
        case "package_purchase" =>
          val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
          val packagePurchaseExtras = spark.read.parquet(appConfig.getString("Path.PackagePurchase"))
            .filter(col("amount") > lit(0) && col("cnt") > lit(0))
            .withColumn(month_index, monthIndexOfUDF(col("date")))
            .drop("date_key")

          packagePurchaseExtras
      }
    }
  */

  private val readPackagePurchase: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package_purchase" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val packagePurchase = spark.read.parquet(appConfig.getString("Path.PackagePurchase"))
          .filter(col("amount") > lit(0) && col("cnt") > lit(0))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))

          .drop("date_key")

        packagePurchase.filter(col("service_type") === "Pay Bill").show()


        packagePurchase
    }
  }

  private val readLoanAssign: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "loan_assign" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val loanAssignDf = spark.read.parquet(appConfig.getString("Path.LoanAssign"))
          .filter(col("bib_id").isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("dt", unix_timestamp(col("date_key").cast("string"), "yyyyMMdd").cast("timestamp"))
          .withColumn("dt_sec", unix_timestamp(col("dt").cast("string")))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .drop("date_key")

        loanAssignDf

      case _ =>
        throw new IllegalArgumentException(s"Unknown file type: $fileType")
    }
  }

  private val readLoanRec: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "loan_rec" =>

        println("--- in the loan recovery")
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val recFeat = spark.read.parquet(appConfig.getString("Path.LoanRec"))
          .filter(col("bib_id").isNotNull)
          .withColumn("date_l", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date_l")))
          .withColumn("dt_l", unix_timestamp(col("date_key").cast("string"), "yyyyMMdd").cast("timestamp"))
          .withColumn("dt_sec_l", unix_timestamp(col("dt_l").cast("string")))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .drop("date_key")
          .groupBy("loan_id", bibID).agg(sum("hsdp_recovery").alias("recovered_amt"), max("dt_sec_l").alias("recovered_time"))

        println("--- recFeat created")
        recFeat.printSchema()

        val loanAssignDf = spark.read.parquet(appConfig.getString("Path.LoanAssign"))
          .filter(col("bib_id").isNotNull)
          .withColumn("date_r", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date_r")))
          .withColumn("dt_r", unix_timestamp(col("date_key").cast("string"), "yyyyMMdd").cast("timestamp"))
          .withColumn("dt_sec_r", unix_timestamp(col("dt_r").cast("string")))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .drop("date_key")

        println("--- loanAssignDf created")
        loanAssignDf.printSchema()

        val recoveredLoan = recFeat
          .drop(bibID)
          .drop(nidHash)
          .join(loanAssignDf, Seq("loan_id"), "right").na.fill(Map("recovered_amt" -> 0))

        println("--- recoveredLoan created")
        recoveredLoan.printSchema()

        recoveredLoan.show(10, truncate = false)

        recoveredLoan

      case _ =>
        throw new IllegalArgumentException(s"Unknown file type: $fileType")
    }
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
        val basePath = "/home/yazdan/bnpl-etl/sample/cdr_features_16845_16846/cdr_features_16845_16846"
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val cdr = spark.read.option("basePath", basePath)
          .parquet(basePath)
          .filter(col(bibID).isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .repartition(300)
          .drop("date_key")

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${16847}_${16848}"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)
          .distinct()

        val cdrFiltered = cdr
          .join(changeOwnerships,
            cdr("bib_id") === changeOwnerships(bibID),
            "left_anti")
          .filter(col(bibID).isNotNull)

        cdrFiltered
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

        handsetPrice

    case "handset_price_brands" =>
      val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
      var handsetPrice = spark.read.parquet(appConfig.getString("Path.HandsetPrice"))
        .withColumn("handset_brand_array", array("handset_brand"))
        .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
        .withColumn(month_index, monthIndexOfUDF(col("date")))
        .drop("date_key")

      handsetPrice = handsetPrice.dropDuplicates()
      handsetPrice = handsetPrice.filter(col("handset_brand").isNotNull)
      handsetPrice = handsetPrice.filter(col("cnt_of_days").isNotNull)

      handsetPrice
    }
  }

  private val readArpu: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "arpu" =>

        val w = Window.partitionBy("fake_msisdn").orderBy(month_index)
        val wCount = Window.partitionBy("fake_msisdn")
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val arpu = spark.read.parquet(appConfig.getString("Path.Arpu"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("month_id")
          .drop("date_key")
          .dropDuplicates()
          .na.fill(0)
          .withColumn("dense_rank", dense_rank().over(w))
          .withColumn("count_dense_rank", size(collect_set("dense_rank").over(wCount)))

        arpu.filter(col("fake_msisdn") === "0CA5143503557C9879D14DE325D710A3").show(false)
        Thread.sleep(3000)

        arpu
    }
  }

  private val readArpuChanges: (String, Int) => DataFrame = { (fileType: String, index: Int) =>

    fileType match {
      case "arpu_changes" =>

        val w = Window.partitionBy("fake_msisdn").orderBy(month_index)

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val arpu = spark.read.parquet(appConfig.getString("Path.Arpu"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("month_id")
          .drop("date_key")
          .dropDuplicates()
          .withColumn("dense_rank", dense_rank().over(w))

        arpu
    }
  }

  private val readBankInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>

    fileType match {
      case "customer_person_type_bank_info" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val iranianBanks = Seq(
          "mellat", "tejarat", "keshavarzi", "refah", "melli",
          "pasargad", "maskan", "resalat", "ayandeh", "parsian",
          "enbank", "sina", "iz"
        )

        val extractBankUDF = udf { name: String =>
          if (name == null || name.toLowerCase.trim.startsWith("v.")) Seq.empty[String]
          else {
            val normalized = name.toLowerCase.replaceAll("[^a-z0-9\\s]", "")
            iranianBanks.filter(bank => normalized.contains(bank))
          }
        }

        val rawBankInfo = spark.read.parquet(appConfig.getString("Path.BankInfo"))
          .filter(col("fake_msisdn").isNotNull)

        val changeOwnerships = spark.read.parquet("/home/erfan/Desktop/Change_ownership_list/drop_list_16848_16849")
          .dropDuplicates("bib_id", "nid_hash")
          .select("bib_id")
          .distinct()

        val bankInfo = rawBankInfo
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("matched_banks", extractBankUDF(col("bank_name")))
          .filter(size(col("matched_banks")) > lit(0))

        val bankInfoFiltered = bankInfo
          .join(changeOwnerships,
            bankInfo("fake_msisdn") === changeOwnerships("bib_id"),
            "left_anti")

        val cvModel = new CountVectorizerModel(iranianBanks.toArray)
          .setInputCol("matched_banks")
          .setOutputCol("bank_vector")

        val vectorized = cvModel.transform(bankInfoFiltered)

        val afterCount = bankInfoFiltered.count()
        println(s"After filtering (bankInfoFiltered) count: $afterCount")
        val beforeCount = bankInfo.count()
        println(s"Before filtering (bankInfo) count: $beforeCount")
        println(s"Dropped rows: ${beforeCount - afterCount}")
//        Thread.sleep(10000)

        vectorized
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
