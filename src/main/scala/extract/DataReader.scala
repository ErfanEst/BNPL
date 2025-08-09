package extract

import core.Core.SourceCol.Arpu.{averageAge, flagSimTierMode, genderMode, mostFrequentFlagSimTier, mostFrequentGender, siteTypeMode}
import core.Core.{appConfig, logger, spark}
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, to_date, to_timestamp, unix_timestamp}
import org.apache.spark.sql.types.IntegerType
import task.FeatureMaker.index
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.monthIndexOf
import org.apache.spark.storage.StorageLevel

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
    case x if List("handset_price").contains(x) => readHandSetPrice(x, index)
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

        val neededCols = Seq(
          "fake_msisdn", "credit_amount", "credit_assigned_date", "loan_id", "loan_amount", "loan_status", "installment_id", "installment_amount", "installment_duedate", "days_delayed", "date_key"
        )

        val basePath = appConfig.getString("Path.CreditManagement")

        val previousMonth = index - 1
        val creditPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_CREDIT_MANAGEMENT",
          s"$basePath/$index/DEFAULT.BNPL_CREDIT_MANAGEMENT"
        )

        val creditRaw = spark.read.parquet(creditPaths: _*)
          .select(neededCols.map(col): _*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .filter(col("credit_assigned_date") > "2024-12-16")

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

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti-join to avoid shuffle
        val creditFiltered = result
          .join(broadcast(changeOwnerships), result("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .filter(col("fake_msisdn").isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + creditFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + creditFiltered.rdd.toDebugString)

        creditFiltered

    }
  }

  private val readPostPaid: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "post_paid" =>

        val w = Window.partitionBy("fake_msisdn").orderBy(month_index)

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq(
          "fake_msisdn", "credit_limit", "deposit_amt_n", "outstanding_balance", "unbilled_amount", "last_status", "last_payment_date", "avl_credit_limit", "suspension_flag", "month_id", "date_key"
        )

        val basePath = appConfig.getString("Path.PostPaid")

        val previousMonth = index - 1
        val poatpaidPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_POSTPAID_CREDIT_HISTORY",
          s"$basePath/$index/DEFAULT.BNPL_POSTPAID_CREDIT_HISTORY"
        )

        val postPaid = spark.read.parquet(poatpaidPaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("row_number", row_number().over(w))

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti-join to avoid shuffle
        val postpaidFiltered = postPaid
          .join(broadcast(changeOwnerships), postPaid("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .filter(col("fake_msisdn").isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)//

        logger.info(s"${fileType} created — count: " + postpaidFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + postpaidFiltered.rdd.toDebugString)

        postpaidFiltered

    }
  }

  private val readDomestic: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {

      case "domestic_travel" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq(
          "fake_msisdn", "sum_travel", "date_key"
        )

        val basePath = appConfig.getString("Path.DomesticTravel")

        val previousMonth = index - 1
        val domesticPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_DOMESTIC_TRAVELERS",
          s"$basePath/$index/DEFAULT.BNPL_DOMESTIC_TRAVELERS"
        )

        val domestic = spark.read.parquet(domesticPaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti-join to avoid shuffle
        val domesticFiltered = domestic
          .join(broadcast(changeOwnerships), domestic("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .filter(col("fake_msisdn").isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + domesticFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + domesticFiltered.rdd.toDebugString)

        domesticFiltered
    }
  }


  private val readPackagePurchase: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package_purchase" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq(
          "fake_msisdn", "service_type", "cnt", "amount", "source_system_cd", "date_key"
        )

        val basePath = appConfig.getString("Path.PackagePurchase")
        val previousMonth = index - 1
        val pkgPurchasePaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_PACKAGE_PURCHASE",
          s"$basePath/$index/DEFAULT.BNPL_PACKAGE_PURCHASE"
        )

        val packagePurchase = spark.read.parquet(pkgPurchasePaths: _*)
          .select(neededCols.map(col):_*)
          .filter(col("amount") > lit(0) && col("cnt") > lit(0))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti-join to avoid shuffle
        val packagePurchaseFiltered = packagePurchase
          .join(broadcast(changeOwnerships), packagePurchase("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .filter(col("fake_msisdn").isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + packagePurchaseFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + packagePurchaseFiltered.rdd.toDebugString)

        packagePurchaseFiltered
    }
  }


  private val readLoanAssign: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "loan_assign" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq("bib_id", "loan_id", "loan_amount", "date_key", "date_timestamp")

        val basePath = appConfig.getString("Path.LoanAssign")
        val previousMonth = index - 1
        val loanAssignPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_AAT_LABS_LOAN_ASSIGNEE",
          s"$basePath/$index/DEFAULT.BNPL_AAT_LABS_LOAN_ASSIGNEE"
        )

        val loanAssign = spark.read.parquet(loanAssignPaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("dt_sec", unix_timestamp(col("date_timestamp"), "yyyyMMdd HH:mm:ss").cast("timestamp"))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .drop("date_key")

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti join to avoid shuffle
        val loanAssignFiltered = loanAssign
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + loanAssignFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + loanAssignFiltered.rdd.toDebugString)

        loanAssignFiltered

    }
  }


  private val readLoanRec: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "loan_rec" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededColsRec = Seq("bib_id", "loan_id", "loan_amount", "hsdp_recovery", "date_timestamp", "date_key")

        val basePathRec = appConfig.getString("Path.LoanRec")
        val previousMonthRec = index - 1
        val loanRecPaths = Seq(
          s"$basePathRec/$previousMonthRec/DEFAULT.BNPL_AAT_LABS_LOAN_REC",
          s"$basePathRec/$index/DEFAULT.BNPL_AAT_LABS_LOAN_REC"
        )

        val recFeat = spark.read.parquet(loanRecPaths: _*)
          .select(neededColsRec.map(col):_*)
          .filter(col("bib_id").isNotNull)
          .withColumn("date_l", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date_l")))
          //          .withColumn("dt_l", unix_timestamp(col("date_key").cast("string"), "yyyyMMdd").cast("timestamp"))
          .withColumn("dt_sec_l", unix_timestamp(col("date_timestamp"), "yyyyMMdd HH:mm:ss").cast("timestamp"))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .drop("date_key")
          .groupBy("loan_id", bibID).agg(sum("hsdp_recovery").alias("recovered_amt"), max("dt_sec_l").alias("recovered_time"))

        val neededColsAssign = Seq("bib_id", "loan_id", "loan_amount", "date_key", "date_timestamp")

        val basePathAssign = appConfig.getString("Path.LoanAssign")
        val previousMonthAssign = index - 1
        val loanAssignPaths = Seq(
          s"$basePathAssign/$previousMonthAssign/DEFAULT.BNPL_AAT_LABS_LOAN_ASSIGN",
          s"$basePathAssign/$index/DEFAULT.BNPL_AAT_LABS_LOAN_ASSIGN"
        )

        val loanAssignDf = spark.read.parquet(loanAssignPaths: _*)
          .select(neededColsAssign.map(col):_*)
          .filter(col("bib_id").isNotNull)
          .withColumn("date_r", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date_r")))
          //          .withColumn("dt_r", unix_timestamp(col("date_key").cast("string"), "yyyyMMdd").cast("timestamp"))
          .withColumn("dt_sec_r", unix_timestamp(col("date_timestamp"), "yyyyMMdd HH:mm:ss").cast("timestamp"))
          .withColumn("loan_id", col("loan_id").cast("long"))
          .withColumn("loan_amount", col("loan_amount").cast("int"))
          .filter(col(month_index) === index - 1)
          .drop("date_key")

        val recoveredLoan = recFeat
          .drop(bibID)
          .drop(nidHash)
          .join(loanAssignDf, Seq("loan_id"), "right").na.fill(Map("recovered_amt" -> 0))


        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti join to avoid shuffle
        val loanAssignFiltered = recoveredLoan
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + loanAssignFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + loanAssignFiltered.rdd.toDebugString)

        recoveredLoan
    }
  }

  private val readUserInfo: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "user_info" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq(bibID, "contract_type_v", "gender_v", "registration_date_d", "date_of_birth_d", "ability_status", "account_balance", "base_station_cd", "siteid", "date_key")

        val basePath = appConfig.getString("Path.UserInfo")
        val previousMonth = index - 1
        val userInfoPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_AAT_LABS_CUSTOMER_INFO_BALANCE_SITEID",
          s"$basePath/$index/DEFAULT.BNPL_AAT_LABS_CUSTOMER_INFO_BALANCE_SITEID"
        )

        val user = spark.read.parquet(userInfoPaths: _*)
          .select(neededCols.map(col):_*)
          .filter(col(bibID).isNotNull)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti join to avoid shuffle
        val userFiltered = user
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + userFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + userFiltered.rdd.toDebugString)

        userFiltered
    }
  }

  private val readPackage: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "package" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq("bib_id", "offering_code", "offer_amount", "offering_name", "activation_date", "deactivation_date", "date_key")

        val basePath = appConfig.getString("Path.Package")
        val previousMonth = index - 1
        val packagePaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_AAT_LABS_ONLINE_BOLTON",
          s"$basePath/$index/DEFAULT.BNPL_AAT_LABS_ONLINE_BOLTON"
        )

        val pkg = spark.read.parquet(packagePaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("de_a_date", unix_timestamp(to_timestamp(col("deactivation_date"), "yyyyMMdd HH:mm:ss")))
          .withColumn("a_date", unix_timestamp(to_timestamp(col("activation_date"), "yyyyMMdd HH:mm:ss")))

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti join to avoid shuffle
        val pkgFiltered = pkg
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + pkgFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + pkgFiltered.rdd.toDebugString)

        pkgFiltered
    }
  }

  private val readCDR: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "cdr" =>
        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))
        val basePath = appConfig.getString("Path.CDR")
        val previousMonth = index - 1
        val cdrPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_AAT_LABS_CDR",
          s"$basePath/$index/DEFAULT.BNPL_AAT_LABS_CDR"
        )

        logger.info(s"Loading CDR paths: $cdrPaths")

        val neededCols = Seq(
          bibID, "sms_count", "voice_count",
          "call_duration", "gprs_usage", "voice_session_cost", "date_key"
        )

        val cdr = spark.read.parquet(cdrPaths: _*)
          .filter(col(bibID).isNotNull)
          .select(neededCols.map(col): _*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti join to avoid shuffle
        val cdrFiltered = cdr
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info("cdrFiltered created — count: " + cdrFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info("cdrFiltered lineage:\n" + cdrFiltered.rdd.toDebugString)

        cdrFiltered
    }
  }

  private val readHandSetPrice: (String, Int) => DataFrame = { (fileType: String, index: Int) =>

    fileType match {
      case "handset_price" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq("fake_msisdn", "handset_model", "handset_brand", "handset_type", "cnt_of_days", "month_id", "date_key")

        val basePath = appConfig.getString("Path.HandsetPrice")
        val previousMonth = index - 1
        val handsetPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_HANDSET_PRICE",
          s"$basePath/$index/DEFAULT.BNPL_HANDSET_PRICE"
        )

        var handsetPrice = spark.read.parquet(handsetPaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("handset_brand_array", array("handset_brand"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop("date_key")

        val cvm: CountVectorizerModel = new CountVectorizerModel(Array("SAMSUNG", "XIAOMI", "HUAWEI", "APPLE", "Other"))
          .setInputCol("handset_brand_array")
          .setOutputCol("handsetVec")

        handsetPrice = cvm.transform(handsetPrice)

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        // Broadcast anti-join to avoid shuffle
        val handsetFiltered = handsetPrice
          .join(broadcast(changeOwnerships), handsetPrice("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .filter(col("fake_msisdn").isNotNull)
          .filter(col("handset_brand").isNotNull)
          .filter(col("cnt_of_days").isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info(s"${fileType} created — count: " + handsetFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + handsetFiltered.rdd.toDebugString)

        handsetFiltered
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

        val neededCols = Seq("fake_msisdn", "bank_name", "sms_cnt", "date_key")

        val basePath = appConfig.getString("Path.BankInfo")
        val previousMonth = index - 1
        val handsetPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.PERSON_TYPE_AND_BANK_INFO",
          s"$basePath/$index/DEFAULT.PERSON_TYPE_AND_BANK_INFO"
        )

        val iranianBanks = Seq(
          "mellat", "tejarat", "keshavarzi", "refah", "melli",
          "pasargad", "maskan", "resalat", "ayandeh", "parsian",
          "enbank", "sina", "iz"
        )

        val cvModel = new CountVectorizerModel(iranianBanks.toArray)
          .setInputCol("matched_banks")
          .setOutputCol("bank_vector")

        val extractBankUDF = udf { name: String =>
          if (name == null || name.toLowerCase.trim.startsWith("v.")) Seq.empty[String]
          else {
            val normalized = name.toLowerCase.replaceAll("[^a-z0-9\\s]", "")
            iranianBanks.filter(bank => normalized.contains(bank))
          }
        }

        val bankInfo = spark.read.parquet(handsetPaths: _*)
          .select(neededCols.map(col):_*)
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .withColumn("matched_banks", extractBankUDF(col("bank_name")))
          .filter(size(col("matched_banks")) > lit(0))

        val changeOwnerships = spark.read.parquet("/home/yazdan/bnpl-etl/sample/drop_list_16847_16848")
          .dropDuplicates("bib_id", "nid_hash")
          .select("bib_id")
          .distinct()

        val bankInfoFiltered = bankInfo
          .join(broadcast(changeOwnerships), bankInfo("fake_msisdn") === changeOwnerships(bibID), "left_anti")
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)

        val vectorized = cvModel.transform(bankInfoFiltered)

        logger.info(s"${fileType} created — count: " + vectorized.take(1).mkString("Array(", ", ", ")"))
        logger.info(s"${fileType} lineage:\n" + vectorized.rdd.toDebugString)

        vectorized
    }
  }


  private val readRecharge: (String, Int) => DataFrame = { (fileType: String, index: Int) =>
    fileType match {
      case "recharge" =>

        val monthIndexOfUDF = udf((date: String) => monthIndexOf(date))

        val neededCols = Seq(
          bibID, "recharge_value_amt", "recharge_dt", "origin_host_nm", "account_balance_before_amt", "account_balance_after_amt", "date_key"
        )

        val basePath = appConfig.getString("Path.Recharge")
        val previousMonth = index - 1
        val rechPaths = Seq(
          s"$basePath/$previousMonth/DEFAULT.BNPL_AAT_LABS_RECHARGE",
          s"$basePath/$index/DEFAULT.BNPL_AAT_LABS_RECHARGE"
        )

        val recharge = spark.read.parquet(rechPaths: _*)
          .filter(col(bibID).isNotNull)
          .select(neededCols.map(col): _*)
          .withColumn("recharge_dt", to_timestamp(col("recharge_dt"), "yyyyMMdd' 'HH:mm:ss"))
          .withColumn("date", to_date(col("date_key"), "yyyyMMdd"))
          .withColumn(month_index, monthIndexOfUDF(col("date")))
          .drop(col("date_key"))

        val changeOwnershipsPath = s"${appConfig.getString("changeOwnershipPath")}${index - 1}_$index"
        val changeOwnerships = spark.read.parquet(changeOwnershipsPath)
          .dropDuplicates(bibID, nidHash)
          .select(bibID)

        val rechFiltered = recharge
          .join(broadcast(changeOwnerships), Seq(bibID), "left_anti")
          .filter(col(bibID).isNotNull)
          .dropDuplicates()
          .persist(StorageLevel.MEMORY_AND_DISK)// ✅ Materialize this for reuse or costly downstream ops

        logger.info("rechFiltered created — count: " + rechFiltered.take(1).mkString("Array(", ", ", ")"))
        logger.info("rechFiltered lineage:\n" + rechFiltered.rdd.toDebugString)

        recharge
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