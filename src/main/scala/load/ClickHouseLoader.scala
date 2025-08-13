package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import core.Core.{appConfig, logger, spark}

object ClickHouseLoader {

  def loadCDRData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createCDRFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.cdr_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }


    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "CDR_features") // must match the case used in ClickHouse
      .mode(SaveMode.Append)
      .save()

    logger.info("CDR features written to ClickHouse successfully.")
  }

  def loadRechargeData(rechargeDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createRechargeFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.recharge_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = rechargeDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }



    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "recharge_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("Recharge features written to ClickHouse successfully.")
  }

  def loadCreditManagementData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createCreditManagementFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.credit_management_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "credit_management_features") // matches the table created earlier
      .mode(SaveMode.Append)
      .save()

    logger.info("Credit Management features written to ClickHouse successfully.")
  }
  def loadUserInfoData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createUserInfoFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.user_info_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "userinfo_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("UserInfo features written to ClickHouse successfully.")
  }

  def loadDomesticTravelData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createDomesticTravelFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.domestic_travel_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "domestic_travel_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("Domestic Travel features written to ClickHouse successfully.")
  }

  def loadLoanRecData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createLoanRecFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loanrec_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "loanrec_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("LoanRec features written to ClickHouse successfully.")
  }


  def loadLoanAssignData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createLoanAssignFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loanassign_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "loanassign_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("LoanAssign features written to ClickHouse successfully.")
  }

  def loadPackagePurchaseExtrasData(aggregatedDF: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createPackagePurchaseExtrasTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.package_purchase_extras_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = aggregatedDF
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "package_purchase_extras_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("PackagePurchaseExtras features written to ClickHouse successfully.")
  }

  def loadPackagePurchaseData(df: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createPackagePurchaseFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.package_purchase_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = df
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "package_purchase_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("PackagePurchase features written to ClickHouse successfully.")
  }




  def loadPostPaidFeaturesData(df: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createPostPaidFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.post-paid-credit")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = df
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "postpaid_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("PostPaid features written to ClickHouse successfully.")
  }

  def loadBankInfoFeaturesData(df: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createBankInfoFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.bank_info_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = df
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "bankinfo_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("BankInfo features written to ClickHouse successfully.")
  }

  def loadHandsetPriceFeaturesData(df: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createHandsetPriceFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.handset_price")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = df
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "handset_price_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("HandsetPrice features written to ClickHouse successfully.")
  }





  def loadPackageFeaturesData(df: DataFrame): Unit = {
    // Ensure table exists
    TableCreation.createPackageFeaturesTable()

    // Load feature defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.package_features")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)
    val featureDefaults: Map[String, Any] = featureKeys.map { key =>
      val value = featureDefaultsConfig.getAnyRef(key)
      key -> value
    }.toMap

    // Fill missing values with defaults
    var finalDF = df
    featureDefaults.foreach { case (colName, defaultValue) =>
      if (finalDF.columns.contains(colName)) {
        finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultValue)))
      }
    }

    // Write to ClickHouse using JDBC
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "package_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("Package features written to ClickHouse successfully.")
  }

  def loadArpuFeaturesData(df: DataFrame): Unit = {
    // Ensure table exists first
    TableCreation.createArpuFeaturesTable()

    // Register DF as temp view for SQL defaults like AVG(age_1)
    df.createOrReplaceTempView("finalDF_view")

    // Load defaults from config
    val featureDefaultsConfig = appConfig.getConfig("featureDefaults.arpu")
    val featureKeys = featureDefaultsConfig.entrySet().toArray.map(_.toString.split("=")(0).trim)

    var finalDF = df

    featureKeys.foreach { rawKey =>
      val key = rawKey.stripPrefix(""").stripSuffix(""") // remove quotes
      val configValue = featureDefaultsConfig.getString(rawKey)

      val replacementCol =
        if (configValue.trim.toUpperCase.startsWith("SELECT")) {
          // Execute SQL default
          val computedVal = spark.sql(configValue).first().get(0)
          lit(computedVal)
        } else {
          // Static numeric default
          if (configValue.contains(".")) lit(configValue.toDouble)
          else lit(configValue.toInt)
        }

      if (finalDF.columns.contains(key)) {
        finalDF = finalDF.withColumn(key, coalesce(col(key), replacementCol))
      }
    }

    // Write to ClickHouse
    finalDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", "arpu_features")
      .mode(SaveMode.Append)
      .save()

    logger.info("ARPU features written to ClickHouse successfully.")
  }





}


