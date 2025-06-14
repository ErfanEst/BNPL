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
    case "month_end_sms_ratio" => first(col("month_end_sms_count") / col("total_all_banks_sms_count"))
    case "bank_loyalty_ratio" => first(col("primary_bank_sms_count") / col("total_all_banks_sms_count"))
    case "isMellat"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "mellat", 1), ignoreNulls = true)
    case "isTejarat"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "tejarat", 1), ignoreNulls = true)
    case "isKeshavarzi" => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "keshavarzi", 1), ignoreNulls = true)
    case "isRefah"      => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "refah", 1), ignoreNulls = true)
    case "isMelli"      => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "melli", 1), ignoreNulls = true)
    case "isPasargad"   => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "pasargad", 1), ignoreNulls = true)
    case "isMaskan"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "maskan", 1), ignoreNulls = true)
    case "isResalat"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "resalat", 1), ignoreNulls = true)
    case "isAyandeh"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "ayandeh", 1), ignoreNulls = true)
    case "isParsian"    => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "parsian", 1), ignoreNulls = true)
    case "isEnbank"     => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "enbank", 1), ignoreNulls = true)
    case "isSina"       => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "sina", 1), ignoreNulls = true)
    case "isIz"         => first(when(col("rank") === 1 && col("matched_banks").getItem(0) === "iz", 1), ignoreNulls = true)
  }

  // Columns required before transformation
  def listNeedBeforeTransform: Seq[String] = Seq("date_key", "sms_cnt", "bank_name", "fake_msisdn")

  // Transformations applied before aggregation
  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val windowSpec = Window.partitionBy("fake_msisdn", month_index).orderBy(desc("total_sms_count"))
    val windowSpec2 = Window.partitionBy("fake_msisdn", month_index)

    Seq(
      ("total_all_banks_sms_count", sum(col("sms_cnt")).over(windowSpec2)),
      ("total_sms_count", col("total_sms_count")),
      ("rank", rank().over(windowSpec)),
      ("month_end_sms_count", sum(when(dayofmonth(to_date(col("date_key"), "yyyyMMdd")).isin(18, 19, 20, 21, 22), col("sms_cnt")).otherwise(0)).over(windowSpec2)),
      ("primary_bank_sms_count", first(when(col("rank") === 1, col("total_sms_count"))).over(windowSpec)),
    )
  }
}