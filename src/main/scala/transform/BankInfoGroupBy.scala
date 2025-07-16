package transform

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Rank
import utils.Utils.CommonColumns.month_index

object BankInfoGroupBy extends DefaultParamsReadable[BankInfoGroupBy] {
  def apply(): BankInfoGroupBy = new BankInfoGroupBy(Identifiable.randomUID("agg"))
}

class BankInfoGroupBy(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

//    case "primary_bank_name" => first(when(col("rank") === 1, col("bank_name")))
    case "total_bank_count" => countDistinct(col("matched_banks").getItem(0))
    case "bank_active_days_count" => countDistinct("date_key")
    case "month_end_sms_ratio" => first(col("month_end_sms_count") / col("total_all_banks_sms_count_Extra"))
    case "bank_loyalty_ratio_first" => first(col("primary_bank_sms_count") / col("total_all_banks_sms_count_Extra"))
    case "bank_loyalty_ratio_both" => sum(
      when(
        col("primary_bank_both_months") === col("bank_name"),
        col("sms_cnt") / col("total_all_banks_sms_count_Extra")
      ).otherwise(0)
    )

  }

  // Columns required before transformation
  def listNeedBeforeTransform: Seq[String] = Seq("date_key", "sms_cnt", "bank_name", "fake_msisdn")

  // Transformations applied before aggregation
  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val windowSpec = Window.partitionBy("fake_msisdn").orderBy(desc("total_sms_count"))
    val windowSpec2 = Window.partitionBy("fake_msisdn", month_index)
    val w2 = Window.partitionBy("fake_msisdn", month_index).orderBy(desc("total_sms_count"))
    val w = Window.partitionBy("fake_msisdn")
    val w3 = Window.partitionBy("fake_msisdn", "bank_name").orderBy(desc("total_sms_count"))

    val monthWindow = Window.partitionBy("fake_msisdn", "month_index")
      .orderBy(col("total_sms_count").desc)
    val overallWindow = Window.partitionBy("fake_msisdn")
      .orderBy(sum(col("total_sms_count")).over(w3))


    Seq(
      ("rank_in_month", row_number().over(monthWindow)),
      ("primary_bank_one_month",  first(when(col("rank_in_month") === 1, col("bank_name")), ignoreNulls = true).over(monthWindow)),
      ("total_sms_cnt_both", sum(col("total_sms_count")).over(w3)),
      ("rank_overall", row_number().over(Window.partitionBy("fake_msisdn").orderBy(col("total_sms_cnt_both").desc))),
      ("primary_bank_both_months", first(when(col("rank_overall") === 1, col("bank_name")), ignoreNulls = true).over(w)),

      ("total_all_banks_sms_count_Extra", sum(col("sms_cnt")).over(windowSpec2)),
//      ("total_sms_count_per_bank", sum(col("sms_count")).over(windowSpec2)),
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

//    case "temp" => first("total_all_banks_sms_count")
//    case "avg_daily_bank_sms" => mean("total_sms_count")
//    case "isMellat"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "mellat", 1), ignoreNulls = true)
//    case "isTejarat"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "tejarat", 1), ignoreNulls = true)
//    case "isKeshavarzi" => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "keshavarzi", 1), ignoreNulls = true)
//    case "isRefah"      => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "refah", 1), ignoreNulls = true)
//    case "isMelli"      => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "melli", 1), ignoreNulls = true)
//    case "isPasargad"   => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "pasargad", 1), ignoreNulls = true)
//    case "isMaskan"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "maskan", 1), ignoreNulls = true)
//    case "isResalat"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "resalat", 1), ignoreNulls = true)
//    case "isAyandeh"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "ayandeh", 1), ignoreNulls = true)
//    case "isParsian"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "parsian", 1), ignoreNulls = true)
//    case "isEnbank"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "enbank", 1), ignoreNulls = true)
//    case "isSina"       => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "sina", 1), ignoreNulls = true)
//    case "isIz"         => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "iz", 1), ignoreNulls = true)