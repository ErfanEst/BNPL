package transform

import core.Core.SourceCol.Recharge.{date, rechargeDt, rechargeValueAmt}
import org.apache.spark.sql.Column
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, nidHash}


object Recharge extends DefaultParamsReadable[Recharge] {
  def apply(): Recharge = new Recharge(Identifiable.randomUID("agg"))
}

class Recharge(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "count_recharge" => count(bibID)
    case "ratio_afternoon_recharge" => sum(when(col("recharge_hour") > 15, 1).otherwise(0)) / count(nidHash)
    case "max_recharge" => max(rechargeValueAmt)
    case "min_recharge" => min(rechargeValueAmt)
    case "mean_recharge" => mean(rechargeValueAmt)
    case "sum_recharge" => sum(rechargeValueAmt)
    case "balance_max" => max("account_balance_after_amt")
    case "mean_balance_before_recharge" => mean("account_balance_before_amt")
    case "ratio_weekend_recharge" =>
      sum(when(col("wd_de") === "weekend", col(rechargeValueAmt)).otherwise(0)) / sum(rechargeValueAmt)
  }

  def listNeedBeforeTransform: Seq[String] = Seq(rechargeValueAmt, rechargeDt, date)

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    Seq(
      "recharge_hour" -> when(col(rechargeValueAmt) > lit(0), hour(col(rechargeDt))),
      "afternoon" -> when(col("recharge_hour") > lit(15), lit(1)).otherwise(lit(0)),
      "weekday" -> dayofweek(col(date)),
      "wd_de" -> when((col("weekday") === 5) || (col("weekday") === 6), lit("weekend")).otherwise(lit("weekday")),
      "recharge_sum_1_week" -> when(col("wd_de") === "weekend", col(rechargeValueAmt)).otherwise(0)
    )
  }


}
