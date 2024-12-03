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
  }

  def aggregate(
                 name: String,
                 indices: Seq[Int],
                 outputColumns: Map[Int, Seq[String]],
                 index: Int
               )
  : Seq[DataFrame] = {

    println("in the aggregate")

    val maxRange: Int = outputColumns.keys.max

    val allOutputCols: Array[String] = outputColumns.values.flatten.toArray.distinct

    println("all output columns are :" + allOutputCols.mkString("Array(", ", ", ")"))

    val aggregator: AbstractAggregator = selectAggregator(name)

    val allNeededCols: Seq[String] = aggregator
      .setRange(maxRange)
      .setMonthIndices(indices)
      .setOutputColumns(allOutputCols)
      .getInputColumns

    println("all Needed Cols are :" + allNeededCols)

    val source =
      name match {
        case _ => selectCols(setTimeRange(selectReader(name, featureTableMap, index))(indices, maxRange))(allNeededCols ++ Seq(nidHash))
      }

    source.show(20)
    println("This is the source data")

    name match {
      case "PackagePurchase" => outputColumns.toSeq.sortBy(_._1).map { case (range, cols) =>
        aggregator
          .copy(ParamMap.empty).asInstanceOf[AbstractAggregator]
          .setRange(range)
          .setOutputColumns(cols.toArray)
          .transformPackagePurchase(source)
      case _ => outputColumns.toSeq.sortBy(_._1).map { case (range, cols) =>
        aggregator
          .copy(ParamMap.empty).asInstanceOf[AbstractAggregator]
          .setRange(range)
          .setOutputColumns(cols.toArray)
          .transform(source)
      }

      }
    }
  }
}
