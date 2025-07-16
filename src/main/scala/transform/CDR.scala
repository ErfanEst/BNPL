package transform


import core.Core.SourceCol.CDR.{CallDuration, GprsUsage, SMSCount, VoiceCount}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, dateKey, month_index}

object CDR extends DefaultParamsReadable[CDR] {
  def apply(): CDR = new CDR(Identifiable.randomUID("agg"))
}

class CDR(override val uid: String) extends AbstractAggregator {

  private val TimeGapSMS = "_time_gap_sms_"
  private val TimeGapVoice = "_time_gap_voice_"
  private val TimeGapGPRS = "_time_gap_gprs_"

  override def aggregator(name: String): Column = name match {
    case "sms_count" => coalesce(sum(col(SMSCount)), lit(0))
    case "voice_count" => coalesce(sum(col(VoiceCount)), lit(0))

    case "call_duration_sum" => coalesce(sum(col(CallDuration)), lit(0))
    case "gprs_usage_sum" => coalesce(sum(col(GprsUsage)), lit(0))

    case "sms_activedays" =>
      countDistinct(when(col(SMSCount) > lit(0), col(dateKey)))
    case "voice_activedays" =>
      countDistinct(when(col(VoiceCount) > lit(0), col(dateKey)))
    case "gprs_usage_activedays" =>
      countDistinct(when(col(GprsUsage) > lit(0), col(dateKey)))

    case "mean_time_interval_sms" => mean(col(TimeGapSMS))
    case "min_time_interval_sms" => min(when(col(TimeGapSMS) > lit(0) && col(TimeGapSMS).isNotNull, col(TimeGapSMS)))
    case "max_time_interval_sms" => max(col(TimeGapSMS))

    case "mean_time_interval_voice" => mean(col(TimeGapVoice))
    case "min_time_interval_voice" => min(when(col(TimeGapVoice) > lit(0) && col(TimeGapVoice).isNotNull, col(TimeGapVoice)))
    case "max_time_interval_voice" => max(col(TimeGapVoice))

    case "mean_time_interval_gprs" => mean(col(TimeGapGPRS))
    case "min_time_interval_gprs" => min(when(col(TimeGapGPRS) > lit(0) && col(TimeGapGPRS).isNotNull, col(TimeGapGPRS)))
    case "max_time_interval_gprs" => max(col(TimeGapGPRS))

    case "sms_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(SMSCount)))
    case "voice_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(VoiceCount)))
    case "call_duration_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(CallDuration)))
    case "gprs_usag_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(GprsUsage)))
    case "ratio_weekend_sms_count" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(SMSCount))) /
        sum(col(SMSCount))
    case "ratio_weekend_voice_count" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(VoiceCount))) /
        sum(col(VoiceCount))
    case "ratio_weekend_call_duration_sum" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(CallDuration))) /
        sum(col(CallDuration))
    case "ratio_weekend_gprs_usage" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(GprsUsage))) /
        sum(col(GprsUsage))
  }

  override def listNeedBeforeTransform: Seq[String] = Seq(dateKey, SMSCount, CallDuration, GprsUsage)

  override def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy(bibID, month_index).orderBy(dateKey).rowsBetween(Window.unboundedPreceding, Window.currentRow - 1)

    val lagsms = when(col(SMSCount) > lit(0), max(when(col(SMSCount) > lit(0), col(dateKey))).over(w))
    val lagvoice = when(col(VoiceCount) > lit(0), max(when(col(VoiceCount) > lit(0), col(dateKey))).over(w))
    val laggprs = when(col(GprsUsage) > lit(0), max(when(col(GprsUsage) > lit(0), col(dateKey))).over(w))

    val weekday = dayofweek(col(dateKey))
    val weekdayOrWeekend = when(weekday === 5 || weekday === 6, "weekend").otherwise("weekday")

    Seq(
      TimeGapSMS -> (unix_timestamp(col(dateKey), "yyyy-MM-dd HH:mm:ss") - unix_timestamp(lagsms, "yyyy-MM-dd HH:mm:ss")),
      TimeGapVoice -> (unix_timestamp(col(dateKey)) - unix_timestamp(lagvoice)),
      TimeGapGPRS -> (unix_timestamp(col(dateKey)) - unix_timestamp(laggprs)),
      "weekday" -> weekday,
      "weekday_or_weekend" -> weekdayOrWeekend
    )
  }
}