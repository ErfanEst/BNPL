package core


import com.typesafe.config.{Config, ConfigFactory}
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import net.jcazevedo.moultingyaml._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.io.Source

object Core {

  val appConfig: Config = ConfigFactory.load
  private object SparkConfigurator {
    def buildSparkSession(): SparkSession = {
      val sparkConfig = appConfig.getConfig("spark")

      val conf = new SparkConf()

      // Load all spark configs from appConfig (which is already loaded at the top)
      sparkConfig.entrySet().forEach { entry =>
        val key = entry.getKey
        val value = sparkConfig.getString(key)
        conf.set(s"spark.$key", value)
      }

      // Explicitly set master and appName (these two are NOT automatically used by Spark if only set via conf)
      SparkSession.builder()
        .master(sparkConfig.getString("master"))
        .appName(sparkConfig.getString("appName"))
        .config(conf)
        .getOrCreate()
    }
  }

  lazy val spark: SparkSession = SparkConfigurator.buildSparkSession()

  val logger = Logger.getLogger("ETL Process")

  implicit private val yamlTablesFormat: YamlFormat[YamlTableData] = yamlFormat2(YamlTableData)

  case class YamlTableData(name: String, cols: List[String])

  lazy private val tablesYaml = Source.fromResource("tables.yml").mkString.parseYaml.asYamlObject.fields
  lazy val parquetTablesYaml: List[YamlTableData] = tablesYaml(YamlString("parquet_tables")).convertTo[List[YamlTableData]]
  lazy val cols: Map[String, List[String]] = parquetTablesYaml.map(x => (x.name, x.cols)).toMap
  lazy val tableNames: List[String] = parquetTablesYaml.map(_.name)


  implicit private val yamlValAggregationFormat: YamlFormat[YamlValAggregation] = yamlFormat3(YamlValAggregation)

  case class YamlValAggregation(name: String, tables: List[String], features: FeatureRange)

  type FeatureRange = Map[String, List[Int]]
  lazy val colsYaml: Map[YamlValue, YamlValue] = Source.fromResource("cols.yml").mkString.parseYaml.asYamlObject.fields
  lazy val aggregationColsYaml: List[YamlValAggregation] = colsYaml(YamlString("aggregation")).convertTo[List[YamlValAggregation]]
  lazy val featureTableMap: Map[String, List[String]] = aggregationColsYaml.map(x => (x.name, x.tables)).toMap

  class RangedCol(val name: String, val range: Int)

  object RangedCol {
    def apply(name: String, range: Int): String =
      name + "_" + range

    def unapply(column: String): RangedCol = {
      val seq = column.split('_')
      new RangedCol(seq.init.mkString("_"), seq.last.toInt)
    }
  }



  case class IndexedColumn(index: Int, name: String)

  object IndexedColumn {
    def apply(index: Int, name: String): String = s"${index}_$name"

    def unapply(column: String): Option[(Int, String)] = {
      val parts = column.split("_", 2)
      if (parts.length == 2 && parts(0).forall(_.isDigit))
        Some((parts(0).toInt, parts(1)))
      else
        None
    }
  }

  object SourceCol {
    private def columnsExtractor(table: String): List[String] = {
      Core.cols(table)
    }

    object CDR {
      private val conf = columnsExtractor("cdr")
      //      val IsWeekend = "_is_weekend_"
      val SMSCount: String = conf(4)
      val VoiceCount: String = conf(5)
      val CallDuration: String = conf(6)
      val GprsUsage: String = conf(7)
      //      val IsActiveSMS = "isActiveSMS"
      //      val IsActiveCall = "isActiveCall"
      //      val IsActiveGPRS = "isActiveGPRS"

    }

    object Package{
      private val conf = columnsExtractor("package")
      val OfferingCode: String = conf(5)
      val OfferAmount: String = conf(6)
      val OfferingName: String = conf(7)
      val ActivationDate: String = "a_date"
      val DeactivationDate: String = "de_a_date"
    }

    object Arpu {
      var flagSimTierMode: DataFrame = _
      var genderMode: DataFrame = _
      var siteTypeMode: DataFrame = _
      var averageAge: Int = _
      var mostFrequentGender: Int = _
      var mostFrequentFlagSimTier: Any = _
    }

    object Recharge {
      private val conf = columnsExtractor("recharge")
      val rechargeValueAmt: String = conf(5)
      val rechargeDt: String = conf(6)
      val date: String = "date"

    }
  }

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    banner(
      """
          Input needed datas:
          --date-- Your desired timeframe for obtaining the features
          --name-- The desired feature name based on the dataset
          """.stripMargin)

    val date: ScallopOption[Int] = opt[Int](required = true, descr = "Date Fathi format")
    val name: ScallopOption[String] = opt[String](required = true, descr = "Name of the aggregation")

    verify()
  }
}