package transform

import core.Core.featureTableMap
import extract.DataReader.{selectCols, selectReader, setTimeRange}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import utils.Utils.CommonColumns.nidHash


object Aggregate {

  def selectAggregator(name: String): AbstractAggregator = name match {
    case "Package" => Package()
    case "CDR" => CDR()
    case "UserInfo" => UserInfo()
    case "PackagePurchase" => PackagePurchase()
    case "HandsetPrice" => HandsetPrice()
    case "Arpu" => Arpu()
    case "BankInfo" => BankInfo()
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

    def getSource(name: String, featureTableMap: Map[String, List[String]], index: Int, indices: Seq[Int], maxRange: Int, allNeededCols: Seq[String], nidHash: String): DataFrame = {

      val commonCols = allNeededCols ++ Seq(if (name == "PackagePurchase" || name == "HandsetPrice" || name == "Arpu") "fake_ic_number" else nidHash)
      val reader = selectReader(name, featureTableMap)
      val df = selectCols(setTimeRange(reader)(indices, maxRange))(commonCols)
      df.show(20, truncate = false)
      println("in the get source ------------")
      Thread.sleep(10000)
      df
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

    // Main logic

    val source = getSource(name, featureTableMap, index, indices, maxRange, allNeededCols, nidHash)
    val result = transformOutput(name, source, outputColumns, aggregator)
    result
  }
}
