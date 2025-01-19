package transform

import core.Core.SourceCol.Arpu.{flagSimTierMode, genderMode, siteTypeMode}
import core.Core.{IndexedColumn, RangedCol}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr, lit}
import utils.Utils.CommonColumns.{month_index, nidHash}
import utils.Utils.getLeafNeededColumns
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.array
import org.apache.spark.sql.functions

abstract class AbstractAggregator extends AbstractTransformer{

  val _range = new IntParam(this, "range", "")

  def setRange(value: Int): AbstractAggregator = set(_range, value)

  def aggregator(name: String): Column

  def listNeedBeforeTransform: Seq[String]

  def listProducedBeforeTransform: Seq[(String, Column)]

  override def getInputColumns: Seq[String] = {

    val needFromOutside = finalOutputColumns.flatMap(getLeafNeededColumns).distinct.diff(listProducedBeforeTransform.map(_._1))
    (Seq(month_index) ++ listNeedBeforeTransform.map(expr).flatMap(getLeafNeededColumns) ++ needFromOutside).distinct
  }

  protected def finalOutputColumns: Array[Column] =
    $(outputCols).map(column => aggregator(column) as RangedCol(column, $(_range)))


  protected def explodeForIndices(dataFrame: DataFrame): DataFrame = {
    if ($(_indices).length == 1) {
      val monthIndex = $(_indices).head
      dataFrame
        .where(col(month_index) > monthIndex - $(_range))
        .withColumn(month_index, lit(monthIndex))
    } else {
      val shiftedMonths = functions.transform(lit(Array.range(0, $(_range))), _ + col(month_index))
      dataFrame.withColumn(month_index, explode(filter(shiftedMonths, _.isin($(_indices): _*))))
    }
  }

  override def selectTransform(name: String, dataset: Dataset[_]): DataFrame = {
    name match {
      case "PackagePurchase" => transformPackagePurchase(dataset)
      case "Arpu" => transformArpu(dataset)
      case "HandsetPrice" => transformHandsetPrice(dataset)
      case "BankInfo" => transformBankInfo(dataset)
      case "BankInfoGroupBy" => transformBankInfoGroupBy(dataset)
      case "Recharge" => transformRecharge(dataset)
      case _ => transform(dataset)
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(nidHash)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }
  /** Second transformation logic */
  def transformPackagePurchase(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number","service_type")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }

  def transformArpu(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val tempDf = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val arpuCustomerWithJoins = tempDf
      .join(siteTypeMode, Seq("fake_ic_number"), "left")
      .join(flagSimTierMode, Seq("fake_ic_number"), "left")
      .join(genderMode, Seq("fake_ic_number"), "left")

    arpuCustomerWithJoins.printSchema()

    arpuCustomerWithJoins
  }

  def transformBankInfo(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
          .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val BankInfo = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

//    val bankMetricsDF = dataset.toDF().groupBy("fake_ic_number")
//      .pivot(month_index, $(_indices))
//      .agg(sum("sms_cnt").as("bank_sms_count"))
//    bankMetricsDF.printSchema()
//
//    val cleanedDataset = dataset.toDF
//      .filter(col("fake_ic_number").isNotNull && col("bank_name").isNotNull && col("sms_cnt").isNotNull)
//
//    // Group by fake_ic_number and bank_name to compute total_sms_cnt
//    val groupedDataset = cleanedDataset.groupBy("fake_ic_number", "bank_name")
//      .agg(sum("sms_cnt").as("total_sms_cnt"))
//
//    // Define windows for ranking (descending for primary and ascending for last)
//    val rankDescWindow = Window.partitionBy("fake_ic_number").orderBy(desc("total_sms_cnt"))
//    val rankAscWindow = Window.partitionBy("fake_ic_number").orderBy(asc("total_sms_cnt"))
//
//    // Add rank, primary bank, and last bank information
//    val rankedDf = groupedDataset
//      .withColumn("rank_desc", row_number().over(rankDescWindow)) // Rank descending by total_sms_cnt
//      .withColumn("rank_asc", row_number().over(rankAscWindow))   // Rank ascending by total_sms_cnt
//      .withColumn("primary_bank", first(when(col("rank_desc") === 1, col("bank_name"))).over(rankDescWindow))
//      .withColumn("primary_bank_sms_count", first(when(col("rank_desc") === 1, col("total_sms_cnt"))).over(rankDescWindow))
//      .withColumn("last_bank", first(when(col("rank_asc") === 1, col("bank_name"))).over(rankAscWindow))
//      .withColumn("last_bank_sms_count", first(when(col("rank_asc") === 1, col("total_sms_cnt"))).over(rankAscWindow))
//
//    // Calculate total_banks_count and sum of sms_cnt for each fake_ic_number
//    val totalsDf = cleanedDataset.groupBy("fake_ic_number")
//      .agg(
//        countDistinct("bank_name").as("total_banks_count"), // Count of unique banks
//        sum("sms_cnt").as("sum_sms")                       // Total SMS count
//      )
//
//    // Join rankedDf with totalsDf to compute loyality2PrimaryBank
//    val resultDf = rankedDf
//      .join(totalsDf, "fake_ic_number")
//      .withColumn("loyality2PrimaryBank", col("primary_bank_sms_count") / col("sum_sms")) // Loyalty calculation
//      .select(
//        "fake_ic_number",
//        "primary_bank",
//        "primary_bank_sms_count",
//        "last_bank",
//        "last_bank_sms_count",
//        "total_banks_count",
//        "loyality2PrimaryBank"
//      )
//      .distinct()

    BankInfo
  }

  private def transformBankInfoGroupBy(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }


  def transformHandsetPrice(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    println(nonMonthIndexDependentDf)

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }

  def transformRecharge(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(nidHash)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

  }
}

