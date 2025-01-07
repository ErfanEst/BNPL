package transform

import core.Core.SourceCol.Package.{ActivationDate, DeactivationDate, OfferAmount, OfferingCode, OfferingName}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BankInfoGroupBy extends DefaultParamsReadable[BankInfo] {
  def apply(): BankInfoGroupBy = new BankInfoGroupBy(Identifiable.randomUID("agg"))
}

class BankInfoGroupBy(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "active_days_cnt" => countDistinct("date_key")
    case "primary_bank" => first(col("bank_name"))
    case "primary_bank_sms_count" => max(col("bank_sms_cnt"))
    case "last_bank" => last(col("bank_name"))
    case "last_bank_sms_count" => min(col("bank_sms_cnt"))
    case "primary_simcard_banks_count" =>
      countDistinct(when(col("fake_msisdn") === col("primary_simcard"), col("bank_name")))
    case "loyality2PrimaryBank" => max("primary_bank_sms_count") / sum("sms_cnt")
  }

  // Columns required before transformation
  def listNeedBeforeTransform: Seq[String] = Seq("date_key", "sms_cnt", "bank_name", "fake_msisdn")

  // Transformations applied before aggregation
  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(

      "bank_sms_cnt" -> col("sms_cnt"),
      "primary_bank_sms_count" -> col("bank_sms_cnt"),
      // Alias for clarity in aggregations
      "primary_simcard" -> first("fake_msisdn").over(Window.partitionBy("fake_ic_number"))
    )
  }

}
