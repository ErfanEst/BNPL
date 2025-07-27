package transform

import core.Core.{featureTableMap, logger}
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
    case "PackagePurchaseExtras" => PackagePurchaseExtras()
    case "HandsetPrice" => HandsetPrice()
    case "HandsetPriceBrands" => HandsetPriceBrands()
    case "Arpu" => Arpu()
    case "ArpuChanges" => ArpuChanges()
    case "BankInfo" => BankInfo()
    case "BankInfoGroupBy" => BankInfoGroupBy()
    case "Recharge" => Recharge()
    case "LoanAssign" => LoanAssign()
    case "LoanRec" => LoanRec()
    case "DomesticTravel" => DomesticTravel()
    case "PostPaid" => PostPaid()
    case "CreditManagement" => CreditManagement()
  }
  def aggregate(
                 name: String,
                 indices: Seq[Int],
                 outputColumns: Map[Int, Seq[String]],
                 index: Int
               )
  : Seq[DataFrame] = {

    val maxRange: Int = outputColumns.keys.max

    logger.info("maxRange is: " + maxRange)

    val allOutputCols: Array[String] = outputColumns.values.flatten.toArray.distinct
    logger.info("allOutputCols is: " + allOutputCols.mkString("Array(", ", ", ")"))


    val aggregator: AbstractAggregator = selectAggregator(name)

    val allNeededCols: Seq[String] = aggregator
      .setRange(maxRange)
      .setMonthIndices(indices)
      .setOutputColumns(allOutputCols)
      .getInputColumns

    logger.info("allNeededCols are: ")
    allNeededCols.foreach(x => logger.info(x))

    def getSource(name: String, featureTableMap: Map[String, List[String]], index: Int, indices: Seq[Int], maxRange: Int, allNeededCols: Seq[String], bibID: String): DataFrame = {
      val commonCols = allNeededCols ++ Seq(if (name == "DomesticTravel" || name == "PackagePurchaseExtras" || name == "PackagePurchase" || name == "HandsetPrice"|| name == "HandsetPriceBrands" || name == "Arpu" || name == "ArpuChanges" || name == "BankInfo" || name == "BankInfoGroupBy" || name == "PostPaid" || name == "CreditManagement") "fake_msisdn" else bibID)
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
//    source.unpersist(true)

    result
  }

}
