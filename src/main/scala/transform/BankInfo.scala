package transform

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Rank
import utils.Utils.CommonColumns.month_index

object BankInfo extends DefaultParamsReadable[BankInfo] {
  def apply(): BankInfo = new BankInfo(Identifiable.randomUID("agg"))
}

class BankInfo(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

    case "total_bank_count" => countDistinct(col("matched_banks").getItem(0))
    case "bank_active_days_count" => countDistinct("date_key")
    case "month_end_sms_ratio" => first(col("month_end_sms_count") / col("total_all_banks_sms_count_Extra"))
    case "bank_loyalty_ratio_first" => first(col("primary_bank_sms_count") / col("total_all_banks_sms_count_Extra"))
    case "avg_daily_bank_sms_first" => sum("sms_cnt")/30
    case "avg_daily_bank_sms_both" => sum("sms_cnt")/60
    case "bank_loyalty_ratio_both" => sum(
      when(
        col("primary_bank_both_months") === col("bank_name"),
        col("sms_cnt") / col("total_all_banks_sms_count_Extra")
      ).otherwise(0)
    )

  }

  // Columns required before transformation
  def listNeedBeforeTransform: Seq[String] = Seq()

  // Transformations applied before aggregation
  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val windowSpec2 = Window.partitionBy("fake_msisdn", month_index)
    val w = Window.partitionBy("fake_msisdn")
    val w3 = Window.partitionBy("fake_msisdn", "bank_name").orderBy(desc("total_sms_count"))

    val monthWindow = Window.partitionBy("fake_msisdn", "month_index")
      .orderBy(col("total_sms_count").desc)

    Seq(
      ("rank_in_month", row_number().over(monthWindow)),
      ("primary_bank_one_month",  first(when(col("rank_in_month") === 1, col("bank_name")), ignoreNulls = true).over(monthWindow)),
      ("total_sms_cnt_both", sum(col("total_sms_count")).over(w3)),
      ("rank_overall", row_number().over(Window.partitionBy("fake_msisdn").orderBy(col("total_sms_cnt_both").desc))),
      ("primary_bank_both_months", first(when(col("rank_overall") === 1, col("bank_name")), ignoreNulls = true).over(w)),

      ("total_all_banks_sms_count_Extra", sum(col("sms_cnt")).over(windowSpec2)),
      ("distinct_total_sms_count", collect_set(col("total_sms_count")).over(w)),
      ("distinct_sum", expr("aggregate(distinct_total_sms_count, 0L, (acc, x) -> acc + x)")),
      ("total_all_banks_sms_count", when(col("distinct_sum").isNotNull, col("distinct_sum"))),
      ("rank", rank().over(monthWindow)),
      ("month_end_sms_count", sum(
        when(dayofmonth(to_date(col("date_key"), "yyyyMMdd")).isin(18, 19, 20, 21, 22), col("sms_cnt"))
          .otherwise(0)
      ).over(windowSpec2)),
      ("primary_bank_sms_count", sum(when(col("rank") === 1, col("sms_cnt"))).over(windowSpec2))
    )
  }
}
