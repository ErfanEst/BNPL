package transform

import core.Core.SourceCol.Package.{ActivationDate, DeactivationDate, OfferAmount, OfferingCode, OfferingName}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object BankInfo extends DefaultParamsReadable[BankInfo] {
  def apply(): BankInfo = new BankInfo(Identifiable.randomUID("agg"))
}

class BankInfo(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "active_days_cnt" => countDistinct("date_key")
    case "max_sms_perDay" => max("sms_cnt")
    case "min_sms_perDay" => min("sms_cnt")
    case "std_sms_perDay" => stddev("sms_cnt")
    case "avg_sms_perDay" => sum("sms_cnt") / countDistinct("date_key")
    case "days_recievedSMS" => count("*")
    case "sum_sms_count" => sum("sms_cnt")
    case "primary_simcard" => first(col("fake_msisdn"))
    case "simcard_count" => countDistinct("fake_msisdn")
    case "total_banks_count" => countDistinct("bank_name")
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
