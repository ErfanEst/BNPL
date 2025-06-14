package transform

import core.Core.SourceCol.Recharge.{date, rechargeDt, rechargeValueAmt}
import org.apache.spark.sql.Column
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, nidHash}


object LoanAssign extends DefaultParamsReadable[LoanAssign] {
  def apply(): LoanAssign = new LoanAssign(Identifiable.randomUID("agg"))
}

class LoanAssign(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "mean_time_interval_loan" => mean("loan_time_interval")
    case "min_time_interval_loan" => min("loan_time_interval")
    case "max_time_interval_loan" => max("loan_time_interval")
    case "count_loans" => countDistinct("loan_id")
    case "loan_amount_sum" => sum("loan_amount")
    case "loan_amount_max" => max("loan_amount")
    case "loan_amount_min" => min("loan_amount")
  }

  def listNeedBeforeTransform: Seq[String] = Seq("dt_sec")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy(bibID).orderBy("dt_sec")
//    val recFeat = Window.partitionBy(bibID, "loan_id")
//    val secs_one_day = 24*3600

    Seq(
      "dt_sec_l_lag" -> lag(col("dt_sec"), 1).over(w),
      "loan_time_interval" -> (col("dt_sec") - col("dt_sec_l_lag")),
//      "recovered_amt" -> sum("hsdp_recovery").over(recFeat),
//      "recovered_time" -> max("dt_sec_r"),
//      "recovered" -> when(col("recovered_amt") === col("loan_amount"), 1).otherwise(0),
//      "time_to_repay" -> (col("recovered_time") - col("dt_sec_l")) / secs_one_day,
    )
  }


}
