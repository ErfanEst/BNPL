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
      case "HandsetPriceBrands" => transformHandsetPriceBrands(dataset)
      case "BankInfo" => transformBankInfo(dataset)
      case "BankInfoGroupBy" => transformBankInfoGroupBy(dataset)
      case "Recharge" => transformRecharge(dataset)
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
//    Thread.sleep(5000)

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
//    Thread.sleep(5000)

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

    val a = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
    val b = a.groupBy("fake_msisdn", "service_type").pivot(month_index, $(_indices))
    val c = b.agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
    val d = c.drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = d.columns.foldLeft(d) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    val result = dynamicGroupBy(renamedDf)
    result
  }

  def transformPostPaid(dataset: Dataset[_]): DataFrame = {

//    val w = Window.partitionBy("fake_msisdn").orderBy(month_index)

//    val df = dataset.toDF().withColumn("row_number", row_number().over(w))

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF())((df, x) => df.withColumn(x._1, x._2))

    nonMonthIndexDependentDf.printSchema()

//    nonMonthIndexDependentDf.filter(col("fake_msisdn") === "DDF58D6723E0DA69DA45FDC01809F8E9").show(10, truncate = false)
//    Thread.sleep(5000)

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))

    result.filter(col("fake_msisdn") === "018B5E24A97654D3029C4CE8DAC57364").show(false)
    println("result =================================")
    Thread.sleep(2000)

    val a = result.groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
    a.filter(col("fake_msisdn") === "018B5E24A97654D3029C4CE8DAC57364").show(false)
    println("a =================================")
    Thread.sleep(5000)

    val b =   a.drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    val renamedDf = b.columns.foldLeft(b) { (df, colName) =>
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



  def transformBankInfo(dataset: Dataset[_]): DataFrame = {
    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
          .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    val BankInfo = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy("fake_msisdn")
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    BankInfo
  }

  private def transformBankInfoGroupBy(dataset: Dataset[_]): DataFrame = {

    val windowSpec = Window.partitionBy("fake_msisdn", "bank_name", month_index)
    val aggDf = dataset
      .withColumn("total_sms_count", sum(col("sms_cnt")).over(windowSpec))

    aggDf.filter(col("fake_msisdn") === "E5C1BC698A695885E001EB00E868B243").show(1000)
    Thread.sleep(10000)

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(aggDf)((df, x) => df.withColumn(x._1, x._2))

    val BankInfo = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))

    BankInfo.filter(col("fake_msisdn") === "E5C1BC698A695885E001EB00E868B243").show(1000)
    Thread.sleep(10000)

    val before =  BankInfo.groupBy("fake_msisdn")

    val temp =  before.pivot(month_index, $(_indices))

    val b =  temp.agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

    b.filter(col("fake_msisdn") === "E5C1BC698A695885E001EB00E868B243").show(1000)
    Thread.sleep(10000)

    val renamedDf = b.columns.foldLeft(b) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf
  }

  def transformHandsetPrice(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    listProducedGrouped.foreach(x => println(x))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF())((df, x) => df.withColumn(x._1, x._2))

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

  def transformHandsetPriceBrands(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    listProducedGrouped.foreach(x => println(x))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF())((df, x) => df.withColumn(x._1, x._2))

    val result = listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))

//    result.show(5, false)
//    Thread.sleep(2000)

    val b = result.groupBy("fake_msisdn", "handset_brand")
    val c = b.pivot(month_index, $(_indices))
    val d = c.agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)

//    d.show(5, false)
//    Thread.sleep(2000)

    val j = d.drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)

//    j.show(20, false)
//    Thread.sleep(2000)

    val renamedDf = j.columns.foldLeft(j) { (df, colName) =>
      IndexedColumn.unapply(colName) match {
        case Some((_, name)) => df.withColumnRenamed(colName, name)
        case None            => df
      }
    }

    renamedDf.show(5)
    Thread.sleep(5000)

    renamedDf
  }

  def transformRecharge(dataset: Dataset[_]): DataFrame = {

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    println(listProducedGrouped)
    println(nonMonthIndexDependentDf)

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }

  def transformLoanAssign(dataset: Dataset[_]): DataFrame = {

    dataset.show(15, truncate = false)

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    println(listProducedGrouped)
    println(nonMonthIndexDependentDf)

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }

  def transformLoanRec(dataset: Dataset[_]): DataFrame = {

    dataset.show(15, truncate = false)

    val listProducedGrouped = listProducedBeforeTransform.groupBy(x => getLeafNeededColumns(x._2).contains(month_index))

    val nonMonthIndexDependentDf =
      listProducedGrouped.getOrElse(false, Map())
        .foldLeft(dataset.toDF)((df, x) => df.withColumn(x._1, x._2))

    println(listProducedGrouped)
    println(nonMonthIndexDependentDf)

    listProducedGrouped.getOrElse(true, Map())
      .foldLeft(explodeForIndices(nonMonthIndexDependentDf))((df, x) => df.withColumn(x._1, x._2))
      .groupBy(bibID)
      .pivot(month_index, $(_indices))
      .agg(first(month_index) as "D_U_M_M_Y", finalOutputColumns: _*)
      .drop($(_indices).map(IndexedColumn(_, "D_U_M_M_Y")): _*)
  }

}

//    val oldColumns = aggDf.columns
//    val monthColumns = oldColumns.drop(3)
//    val newMonthNames = Seq("fake_msisdn", "bank_name", month_index) ++ monthColumns.zipWithIndex.map { case (_, idx) => s"month_${idx + 1}" }
//    val finalDf = aggDf.toDF(newMonthNames: _*)