package transform

import core.Core.SourceCol.Arpu.{flagSimTierMode, genderMode, siteTypeMode}
import core.Core.{IndexedColumn, RangedCol, logger}
import org.apache.spark.ml.feature.CountVectorizerModel
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, expr, lit}
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.getLeafNeededColumns
import org.apache.spark.ml.param.IntParam
import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
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
      case "PackagePurchaseExtras" => transformPackagePurchaseExtras(dataset)
      case "Arpu" => transformArpu(dataset)
      case "ArpuChanges" => transformArpuChanges(dataset)
      case "HandsetPrice" => transformHandsetPrice(dataset)
      case "BankInfo" => transformBankInfo(dataset)
      case "LoanAssign" => transformLoanAssign(dataset)
      case "LoanRec" => transformLoanRec(dataset)
      case "DomesticTravel" => transformDomestic(dataset)
      case "PostPaid" => transformPostPaid(dataset)
      case "CreditManagement" => transformCreditManagement(dataset)
      case _ => transform(dataset)
    }
  }

  def transform(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    logger.info("middle features created!")

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = result.columns.foldLeft(result) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    logger.info("features created!")

    renamedDf
  }

  def transformCreditManagement(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val temp = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)


    val renamedDf = temp.columns.foldLeft(temp) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    val result = dynamicGroupBy(renamedDf)
    result
  }

  def transformPackagePurchase(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF())((df, x) => df.withColumn(x._1, x._2))

    val tmp = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn", "service_type").pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = tmp.columns.foldLeft(tmp) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    val result = dynamicGroupBy(renamedDf)
    result
  }

  def transformPostPaid(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF())((df, x) => df.withColumn(x._1, x._2))


    val postpaid = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = postpaid.columns.foldLeft(postpaid) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformDomestic(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = result.columns.foldLeft(result) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformPackagePurchaseExtras(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = result.columns.foldLeft(result) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def dynamicGroupBy(df: DataFrame, idCol: String = "fake_msisdn"): DataFrame = {

    val featureCols = df.columns.filter(col => col != idCol && col != "service_type")

    val aggregations = featureCols.map { col =>
      if (col.startsWith("min_")) min(col).alias(col)
      else if (col.startsWith("max_")) max(col).alias(col)
      else if (col.startsWith("sum_")) sum(col).alias(col)
      else if (col.startsWith("avg_")) sum(col).alias(col)
      else first(col).alias(col) // fallback
    }

    df.groupBy(idCol).agg(aggregations.head, aggregations.tail: _*)
  }

  def transformArpu(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map.empty)
        .foldLeft(dataset.toDF()) { case (df, (colName, expr)) =>
          df.withColumn(colName, expr)
        }

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))

    result.filter(col("fake_msisdn") === "0DA44CED95D0416B5594951C27FD1370").show(false)
    Thread.sleep(5000)

    val b = result
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    b.filter(col("fake_msisdn") === "0CA5143503557C9879D14DE325D710A3").show(false)
    Thread.sleep(5000)

    val renamedDf = b.columns.foldLeft(b) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformArpuChanges(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map.empty)
        .foldLeft(dataset.toDF()) { case (df, (colName, expr)) =>
          df.withColumn(colName, expr)
        }

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))

    result.filter(col("fake_msisdn") === "0000DFEFEDF9C832D684C5823E7101C6").show(false)
    Thread.sleep(5000)

    val agg = result
      .groupBy("fake_msisdn")
      .agg(
        first("res_com_score_1", true).as("r1"),
        first("res_com_score_2", true).as("r2"),
        first("voice_revenue_1", true).as("v1"),
        first("voice_revenue_2", true).as("v2"),
        first("gprs_revenue_1", true).as("g1"),
        first("gprs_revenue_2", true).as("g2"),
        first("sms_revenue_1", true).as("s1"),
        first("sms_revenue_2", true).as("s2")
      )
      .withColumn("res_com_score_change", col("r1")/col("r2") - lit(1))
      .withColumn("voice_revenue_change",    col("v1")/col("v2") - lit(1))
      .withColumn("gprs_revenue_change",     col("g1")/col("g2") - lit(1))
      .withColumn("sms_revenue_change",      col("s1")/col("s2") - lit(1))


    //      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    //    val renamedDf = b.columns.foldLeft(result) { (df, colName) =>
    //      IndexedColumn.unapply(colName) match {
    //        case Some((_, name)) => df.withColumnRenamed(colName, name)
    //        case None            => df
    //      }
    //    }

    agg
  }

  private def transformBankInfo(dataset: Dataset[_]): DataFrame = {

    val windowSpec = Window.partitionBy("fake_msisdn", "bank_name", month_index)

    val aggDf = dataset
      .withColumn("total_sms_count", sum(col("sms_cnt")).over(windowSpec))

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(aggDf)((df, x) => df.withColumn(x._1, x._2))

    val BankInfo = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)


    val renamedDf = BankInfo.columns.foldLeft(BankInfo) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformHandsetPrice(dataset: Dataset[_]): DataFrame = {

    val windowSpec = Window.partitionBy("fake_msisdn", "handset_model", month_index)
    val aggDf = dataset
      .withColumn("sum_cnt_of_days", sum(col("cnt_of_days"))
        .over(windowSpec))

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    listProducedGrouped.foreach(x => println(x))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(aggDf)((df, x) => df.withColumn(x._1, x._2))

    val handset = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = handset.columns.foldLeft(handset) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }


  def transformRecharge(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = result.columns.foldLeft(result) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformLoanAssign(dataset: Dataset[_]): DataFrame = {



    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val loanAssign = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = loanAssign.columns.foldLeft(loanAssign) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformLoanRec(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val loanRec = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = loanRec.columns.foldLeft(loanRec) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

}