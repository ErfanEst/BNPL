package transform

import core.Core.SourceCol.Recharge.{date, rechargeDt, rechargeValueAmt}
import org.apache.spark.sql.Column
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, dateKey, month_index, nidHash}


object PostPaid extends DefaultParamsReadable[PostPaid] {
  def apply(): PostPaid = new PostPaid(Identifiable.randomUID("agg"))
}

class PostPaid(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

    case "credit_ratio" => first(col("outstanding_balance") / (col("credit_limit") + col("deposit_amt_n")))
    case "unbilled_ratio" => first(col("unbilled_amount") / (col("credit_limit") + col("deposit_amt_n")))
    case "deposit_to_credit_ratio" => first(col("deposit_amt_n") / col("credit_limit"))
    case "has_payment" => first(when(col("last_payment_date").isNull, 0).otherwise(1))
    case "is_creditor" => first(when(col("outstanding_balance") < 0, 1).otherwise(0))
    case "days_since_last_payment" => first(datediff(to_date(col("date_key"), "yyyyMMdd"), col("last_payment_date")))
    case "churn" => first(
      when(
        (datediff(to_date(col("date_key"), "yyyyMMdd"), col("last_payment_date")) > 90) && (col("unbilled_amount") === 0),
        1
      ).otherwise(0)
    )
    case "is_suspended" => first(when(col("suspension_flag").isNull, 0).otherwise(1))
    case "over_limit_flag" => first(when(
      (col("outstanding_balance") + col("unbilled_amount")) > (col("credit_limit") + col("deposit_amt_n")),
      1
    ).otherwise(0))

    case "account_status_active" => first(when(col("last_status").isin("Hard", "Soft"), 0).otherwise(1))

//    case "total_credit_utilization_growth" => max(((col("total_bill_2")/col("total_credit_2"))-(col("total_bill")/col("total_credit")))/(col("total_bill")/col("total_credit")))
//    case "credit_limit_change" => first((col("credit_limit_2") - col("credit_limit"))/ col("credit_limit"))
//    case "credit_limit_growth_rate" => first((col("credit_limit_2") - col("credit_limit")) / col("credit_limit"))
//    case "avl_credit_limit_growth" => first(col("total_credit_2") - col("total_bill_2") - (col("total_credit") + col("total_bill"))/(col("total_credit") - col("total_bill")))
//    case "deposit_change" => first(col("deposit_amt_n_2") - col("deposit_amt_n") / col("deposit_amt_n"))

    case "total_credit_utilization_growth" => first((col("total_bill_2")/col("total_credit_2"))/(col("total_bill")/col("total_credit")) - 1)

    case "credit_limit_change" => max(when(col("row_number") === 2, col("credit_limit_change_2")))
    case "credit_limit_growth_rate" => max(when(col("row_number") === 2, col("credit_limit_growth_rate_2")))
    case "avl_credit_limit_growth" => max(when(col("row_number") === 2, col("avl_credit_limit_growth")))
    case "deposit_change" => max(when(col("row_number") === 2, col("deposit_change_2")))

    case other =>
      throw new IllegalArgumentException(s"Unknown feature name passed to aggregator: '$other'")
  }


  def listNeedBeforeTransform: Seq[String] = Seq("row_number")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy("fake_msisdn").orderBy("row_number")

    val result = Seq(
//      "row_number" -> row_number().over(w),

      "total_credit" -> first(when(col("row_number") === 2, coalesce(col("credit_limit"), lit(0)) + coalesce(col("deposit_amt_n"), lit(0)))).over(w),
      "total_bill" -> first(when(col("row_number") === 2, coalesce(col("outstanding_balance"), lit(0)) + coalesce(col("unbilled_amount"), lit(0)))).over(w),

      "total_credit_2" -> first(when(col("row_number") === 2, coalesce(col("credit_limit"), lit(0)) + coalesce(col("deposit_amt_n"), lit(0)))).over(w),
      "total_bill_2" -> first(when(col("row_number") === 2, coalesce(col("outstanding_balance"), lit(0)) + coalesce(col("unbilled_amount"), lit(0)))).over(w),

      "deposit_amt_n_2" -> first(when(col("row_number") === 2, coalesce(col("deposit_amt_n"), lit(0)))).over(w),
      "credit_limit_2" -> first(when(col("row_number") === 2, coalesce(col("credit_limit"), lit(0)))).over(w),

//      "total_credit_utilization_growth" ->
//        ((last(col("outstanding_balance") + col("unbilled_amount"), ignoreNulls = true).over(w)/first(col("credit_limit") + col("deposit_amt_n"), ignoreNulls = true).over(w))-(col("total_bill")/col("total_credit")))/(col("total_bill")/col("total_credit")),

      "total_credit_utilization_growth_first" -> first((col("total_bill_2")/col("total_credit_2"))/(col("total_bill")/col("total_credit")) - 1).over(w),
      "total_credit_utilization_growth_last" -> last((col("total_bill_2")/col("total_credit_2"))/(col("total_bill")/col("total_credit")) - 1).over(w),

      "avl_credit_limit_growth" ->
        (first(col("credit_limit") + col("deposit_amt_n"), ignoreNulls = true).over(w) - first(col("outstanding_balance") + col("unbilled_amount"), ignoreNulls = true).over(w) - (col("total_credit") + col("total_bill")))/(col("total_credit") - col("total_bill")),

      "deposit_change_2" -> ((first(col("deposit_amt_n")).over(w) - col("deposit_amt_n")) / col("deposit_amt_n")),
      "credit_limit_change_2" -> ((first(col("credit_limit")).over(w) - col("credit_limit")) / col("credit_limit")),
      "credit_limit_growth_rate_2" -> ((first(col("credit_limit")).over(w) - col("credit_limit")) / col("credit_limit")),
    )

    result.foreach(x => println(x))

    result
  }
}
