package utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import core.Core.appConfig

object CDRClickHouseLoader {
  private val spark = SparkSession.builder().getOrCreate()

  def loadCDRData(aggregatedDF: DataFrame, monthIndex: Int): Unit = {
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

    // Add month_index column for ordering
    finalDF = finalDF.withColumn("month_index", lit(monthIndex))

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

    println("CDR features written to ClickHouse successfully.")
  }
}
