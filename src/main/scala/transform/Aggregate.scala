package transform

import core.Core.featureTableMap
import extract.DataReader.{selectCols, selectReader, setTimeRange}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import utils.Utils.CommonColumns.{bibID, nidHash}


object Aggregate {

  def selectAggregator(name: String): AbstractAggregator = name match {
    case "Package" => Package()
    case "CDR" => CDR()
    case "UserInfo" => UserInfo()
    case "PackagePurchase" => PackagePurchase()
    case "HandsetPrice" => HandsetPrice()
    case "Arpu" => Arpu()
    case "BankInfo" => BankInfo()
    case "BankInfoGroupBy" => BankInfoGroupBy()
    case "Recharge" => Recharge()
  }
  def aggregate(
                 name: String,
                 indices: Seq[Int],
                 outputColumns: Map[Int, Seq[String]],
                 index: Int
               )
  : Seq[DataFrame] = {

    val maxRange: Int = outputColumns.keys.max

    val allOutputCols: Array[String] = outputColumns.values.flatten.toArray.distinct

    val aggregator: AbstractAggregator = selectAggregator(name)

    val allNeededCols: Seq[String] = aggregator
      .setRange(maxRange)
      .setMonthIndices(indices)
      .setOutputColumns(allOutputCols)
      .getInputColumns


    def getSource(name: String, featureTableMap: Map[String, List[String]], index: Int, indices: Seq[Int], maxRange: Int, allNeededCols: Seq[String], bibID: String): DataFrame = {
      val commonCols = allNeededCols ++ Seq(if (name == "PackagePurchase" || name == "HandsetPrice" || name == "Arpu" || name == "BankInfo" || name == "BankInfoGroupBy") "fake_ic_number" else bibID)
      val reader = selectReader(name, featureTableMap)
      selectCols(setTimeRange(reader)(indices, maxRange))(commonCols.distinct)
    }

    def transformOutput(
                         name: String,
                         source: DataFrame,
                         outputColumns: Map[Int, Seq[String]],
                         aggregator: AbstractAggregator
                       ): Seq[DataFrame] = {
      outputColumns.toSeq.sortBy(_._1).map { case (range, cols) =>
        aggregator
          .copy(ParamMap.empty)
          .asInstanceOf[AbstractAggregator]
          .setRange(range)
          .setOutputColumns(cols.toArray)
          .selectTransform(name, source)
      }
    }

    val source = getSource(name, featureTableMap, index, indices, maxRange, allNeededCols, bibID)
    val result = transformOutput(name, source, outputColumns, aggregator)
    result
  }
}
