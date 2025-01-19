package transform

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column

object BankInfoGroupBy extends DefaultParamsReadable[BankInfoGroupBy] {
  def apply(): BankInfoGroupBy = new BankInfoGroupBy(Identifiable.randomUID("agg"))
}

class BankInfoGroupBy(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "primary_bank" => first(when(col("rank_desc") === 1, col("bank_name")))
    case "primary_bank_sms_count" => count(when(col("rank_desc") === 1, col("sms_cnt")))
    case "last_bank" => first(when(col("rank_asc") === 1, col("bank_name")))
    case "last_bank_sms_count" => first(when(col("rank_asc") === 1, col("sms_cnt")))
    case "primary_simcard_banks_count" => countDistinct(col("primary_bank_sms_count_temp"))
    case "loyality2PrimaryBank" => max("primary_bank_sms_count_temp") / sum("sms_cnt")
    case _ => throw new IllegalArgumentException(s"Unsupported aggregation name: $name")
  }

  // Columns required before transformation
  def listNeedBeforeTransform: Seq[String] = Seq("date_key", "sms_cnt", "bank_name", "fake_msisdn")

  // Transformations applied before aggregation
  def listProducedBeforeTransform: Seq[(String, Column)] = {
    val rankWindowAsc = Window.partitionBy("bank_name").orderBy(asc("sms_cnt"))
    val rankWindowDsc = Window.partitionBy("bank_name").orderBy(asc("sms_cnt"))
    val primarySimcard = first("fake_msisdn").over(rankWindowAsc)
    val rankAsc = row_number().over(rankWindowAsc)
    val rankDec = row_number().over(rankWindowDsc)
    val primaryBankSMSCount = when(col("fake_msisdn") === col("primary_simcard"), col("bank_name"))

    Seq(
      "rank_desc" -> rankDec,
      "rank_asc" -> rankAsc,
      "primary_simcard" -> primarySimcard,
      "primary_bank_sms_count_temp" -> primaryBankSMSCount
    )
  }
}

