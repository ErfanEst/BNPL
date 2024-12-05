package transform

import core.Core.SourceCol.Arpu.{flagSimTierMode, genderMode, siteTypeMode}
import core.Core.{IndexedColumn, RangedCol, cvm}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr, lit}
import utils.Utils.CommonColumns.{month_index, nidHash}
import utils.Utils.getLeafNeededColumns
import org.apache.commons.math3.stat.descriptive.rank.Percentile
import org.apache.commons.math3.stat.regression.SimpleRegression
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, functions}

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

    println("in the transformArpu")
    dataset.show(truncate = false)

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

    tempDf.show(20, truncate = false)
    println("in transformArpu....")
    Thread.sleep(5000)

    val arpuCustomerWithJoins = tempDf
      .join(siteTypeMode, Seq("fake_ic_number"), "left")
      .join(flagSimTierMode, Seq("fake_ic_number"), "left")
      .join(genderMode, Seq("fake_ic_number"), "left")

    arpuCustomerWithJoins.show(20, truncate = false)
    println("in transformArpu point 2....")
    Thread.sleep(5000)

    val columnName = arpuCustomerWithJoins.columns.find(_.contains("count_active_fake_msisdn")).getOrElse(
      throw new IllegalArgumentException("No column containing 'count_active_fake_msisdn' found")
    )

    val arpuCustomerFiltered = arpuCustomerWithJoins.filter(col(columnName) < 100)

    arpuCustomerFiltered.show(20, truncate = false)
    println("in transformArpu point 3....")
    Thread.sleep(5000)

//    val filledCustomerMetrics = arpuCustomerFiltered
//      .withColumn(
//        "age",
//        when(col("age").isNull, lit(averageAge)).otherwise(col("age"))
//      )
//      .withColumn(
//        "gender",
//        when(col("gender").isNull, lit(mostFrequentGender)).otherwise(col("gender"))
//      )
//      .withColumn(
//        "avg_res_com_score",
//        when(col("avg_res_com_score").isNull, lit(0)).otherwise(col("avg_res_com_score"))
//      )
//      .withColumn(
//        "avg_voice_revenue",
//        when(col("avg_voice_revenue").isNull, lit(0)).otherwise(col("avg_voice_revenue"))
//      )
//      .withColumn(
//        "avg_gprs_revenue",
//        when(col("avg_gprs_revenue").isNull, lit(0)).otherwise(col("avg_gprs_revenue"))
//      )
//      .withColumn(
//        "avg_sms_revenue",
//        when(col("avg_sms_revenue").isNull, lit(0)).otherwise(col("avg_sms_revenue"))
//      )
//      .withColumn(
//        "avg_subscription_revenue",
//        when(col("avg_subscription_revenue").isNull, lit(0)).otherwise(col("avg_subscription_revenue"))
//      )
//      .withColumn(
//        "flag_sim_tier_mode",
//        when(col("flag_sim_tier_mode").isNull, lit(mostFrequentFlagSimTier)).otherwise(col("flag_sim_tier_mode"))
//      )

    arpuCustomerFiltered
  }

  def transformHandsetPrice(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_ic_number")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
    listProducedGrouped
    val featuredDf = cvm.transform(result).drop("handset_names")
    val numBrands = 5 // Number of top brands + 1 for "Other"
    val resultDf = featuredDf.withColumn("handset_v", expr("vector_to_array(handsetVec)"))
      .select(
        col("fake_ic_number") +: (0 until numBrands).map(i => col("handset_v").getItem(i).alias(s"brand_$i")): _*
      )
    resultDf
  }


}
