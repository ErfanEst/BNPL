package utils

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

import scala.jdk.CollectionConverters._

object FillNullLogics {

  def loadFeatureDefaults(config: Config, groupName: String): Map[String, Any] = {
    val groupConfig = config.getConfig(s"featureDefaults.$groupName")
    groupConfig.entrySet().asScala.map { entry =>
      val key = entry.getKey
      val valueString = groupConfig.getString(key)
      // Try to parse as Double or Int
      val value: Any = try {
        valueString.toDouble
      } catch {
        case _: NumberFormatException => try {
          valueString.toInt
        } catch {
          case _: NumberFormatException => valueString
        }
      }
      key -> value
    }.toMap
  }

  def fillNulls(df: DataFrame, defaultMap: Map[String, Any]): DataFrame = {
    var filledDf = df
    for ((colName, defaultValue) <- defaultMap) {
      if (df.columns.contains(colName)) {
        filledDf = filledDf.withColumn(
          colName,
          when(col(colName).isNull, lit(defaultValue)).otherwise(col(colName))
        )
      }
    }
    filledDf
  }

//  def main(args: Array[String]): Unit = {
//    val spark = SparkSession.builder().appName("FillFeatureNulls").getOrCreate()
//    import spark.implicits._
//
//    // Example DataFrame
//    val df = Seq(
//      ("user1", null.asInstanceOf[Integer], 5.0),
//      ("user2", 3, null.asInstanceOf[Double]),
//      ("user3", null.asInstanceOf[Integer], null.asInstanceOf[Double])
//    ).toDF("user_id", "sms_count_1", "voice_count_1")
//
//    // Load config
//    val config = ConfigFactory.load()
//
//    // Choose which group of features to apply (for example, "cdr_features")
//    val defaults = loadFeatureDefaults(config, "cdr_features")
//
//    // Fill nulls
//    val filledDf = fillNulls(df, defaults)
//
//    filledDf.show(truncate = false)
//
//    spark.stop()
//  }
}
