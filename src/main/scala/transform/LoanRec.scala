package transform

import core.Core.SourceCol.Recharge.{date, rechargeDt, rechargeValueAmt}
import org.apache.spark.sql.Column
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, nidHash}


object LoanRec extends DefaultParamsReadable[LoanRec] {
  def apply(): LoanRec = new LoanRec(Identifiable.randomUID("agg"))
}

class LoanRec(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "mean_time_to_repay" => mean("time_to_repay")
  }

  def listNeedBeforeTransform: Seq[String] = Seq("dt_sec_r", "recovered_amt", "loan_amount", "recovered_time")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val secsOneDay = 24*3600

    Seq(
      "recovered" -> when(col("recovered_amt") === col("loan_amount"), 1).otherwise(0),
      "time_to_repay" -> when(col("recovered") === 1, (col("recovered_time") - col("dt_sec_r")) / secsOneDay).otherwise(lit(30)),

    )
  }


}
