package core


import com.typesafe.config.{Config, ConfigFactory}
import net.jcazevedo.moultingyaml.DefaultYamlProtocol._
import net.jcazevedo.moultingyaml._
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.DataFrame

import scala.io.Source

object Core {

  val appConfig: Config = ConfigFactory.load

  val cvm: CountVectorizerModel = new CountVectorizerModel(Array("SAMSUNG", "XIAOMI", "HUAWEI", "APPLE", "Other"))
    .setInputCol("handset_names")
    .setOutputCol("handsetVec")

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

  class IndexedColumn(val index: Int, val rangedName: String)

  object IndexedColumn {
    def apply(index: Int, name: String): String =
      index + "_" + name

    def unapply(column: String): IndexedColumn = {
      val seq = column.split('_')
      new IndexedColumn(seq.head.toInt, seq.tail.mkString("_"))
    }
  }

  object SourceCol {
    private def columnsExtractor(table: String): List[String] = {
      Core.cols(table)
    }

    object CDR {
      private val conf = columnsExtractor("cdr")
      val IsWeekend = "_is_weekend_"
      val SMSCount = conf(4)
      val VoiceCount = conf(5)
      val CallDuration = conf(6)
      val GprsUsage = conf(7)
      val IsActiveSMS = "isActiveSMS"
      val IsActiveCall = "isActiveCall"
      val IsActiveGPRS = "isActiveGPRS"

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
    }
  }
}
