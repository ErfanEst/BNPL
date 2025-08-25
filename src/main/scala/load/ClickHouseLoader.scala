package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import core.Core.{appConfig, logger, spark}

object ClickHouseLoader {

  def loadCDRData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createCDRFeaturesTable(index)




    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"CDR_features_$index") // must match the case used in ClickHouse
      .mode(SaveMode.Append)
      .save()

    logger.info("CDR features written to ClickHouse successfully.")
  }

  def loadRechargeData(rechargeDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createRechargeFeaturesTable(index)


    // Write to ClickHouse using JDBC
    rechargeDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"recharge_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("Recharge features written to ClickHouse successfully.")
  }

  def loadCreditManagementData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createCreditManagementFeaturesTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"credit_management_features_$index") // matches the table created earlier
      .mode(SaveMode.Append)
      .save()

    logger.info("Credit Management features written to ClickHouse successfully.")
  }
  def loadUserInfoData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createUserInfoFeaturesTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"userinfo_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("UserInfo features written to ClickHouse successfully.")
  }

  def loadDomesticTravelData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createDomesticTravelFeaturesTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"domestic_travel_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("Domestic Travel features written to ClickHouse successfully.")
  }

  def loadLoanRecData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createLoanRecFeaturesTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"loanrec_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("LoanRec features written to ClickHouse successfully.")
  }


  def loadLoanAssignData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createLoanAssignFeaturesTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"loanassign_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("LoanAssign features written to ClickHouse successfully.")
  }

  def loadPackagePurchaseExtrasData(aggregatedDF: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createPackagePurchaseExtrasTable(index)


    // Write to ClickHouse using JDBC
    aggregatedDF.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"package_purchase_extras_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("PackagePurchaseExtras features written to ClickHouse successfully.")
  }

  def loadPackagePurchaseData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createPackagePurchaseFeaturesTable(index)


    // Write to ClickHouse using JDBC
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"package_purchase_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("PackagePurchase features written to ClickHouse successfully.")
  }




  def loadPostPaidFeaturesData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createPostPaidFeaturesTable(index)


    // Write to ClickHouse using JDBC
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"postpaid_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("PostPaid features written to ClickHouse successfully.")
  }

  def loadBankInfoFeaturesData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createBankInfoFeaturesTable(index)


    // Write to ClickHouse using JDBC
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"bankinfo_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("BankInfo features written to ClickHouse successfully.")
  }

  def loadHandsetPriceFeaturesData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createHandsetPriceFeaturesTable(index)


    // Write to ClickHouse using JDBC
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"handset_price_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("HandsetPrice features written to ClickHouse successfully.")
  }





  def loadPackageFeaturesData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists
    TableCreation.createPackageFeaturesTable(index)


    // Write to ClickHouse using JDBC
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"package_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("Package features written to ClickHouse successfully.")
  }

  def loadArpuFeaturesData(df: DataFrame, index: Int): Unit = {
    // Ensure table exists first
    TableCreation.createArpuFeaturesTable(index)

    // Write to ClickHouse
    df.write
      .format("jdbc")
      .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
      .option("url", appConfig.getString("clickhouse.url"))
      .option("user", appConfig.getString("clickhouse.user"))
      .option("password", appConfig.getString("clickhouse.password"))
      .option("dbtable", s"arpu_features_$index")
      .mode(SaveMode.Append)
      .save()

    logger.info("ARPU features written to ClickHouse successfully.")
  }





}


