package transform


import core.Core.SourceCol.CDR.{CallDuration, GprsUsage, SMSCount, VoiceCount}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import utils.Utils.CommonColumns.{dateKey, month_index, nidHash}

object CDR extends DefaultParamsReadable[CDR] {
  def apply(): CDR = new CDR(Identifiable.randomUID("agg"))
}

class CDR(override val uid: String) extends AbstractAggregator {

  private val TimeGapSMS = "_time_gap_sms_"
  private val TimeGapCall = "_time_gap_call_"
  private val TimeGapGPRS = "_time_gap_gprs_"

  override def aggregator(name: String): Column = name match {
    case "sms_count" => sum(col(SMSCount).cast("long"))
    case "voice_count" => sum(col(VoiceCount).cast("long"))

    case "call_duration_sum" => sum(col(CallDuration).cast("double"))
    case "gprs_usage_sum" => sum(col(GprsUsage).cast("double"))

    case "sms_activedays" => countDistinct(when(col(SMSCount) > 0, col(nidHash))).cast("double")
    case "voice_activedays" => countDistinct(when(col(VoiceCount) > 0, col(nidHash))).cast("double")
    case "gprs_usage_activedays" => countDistinct(when(col(GprsUsage) > 0, col(nidHash))).cast("double")

    case "mean_time_interval_sms" => org.apache.spark.sql.functions.mean(col(TimeGapSMS)).cast("double")
    case "min_time_interval_sms" => org.apache.spark.sql.functions.min(col(TimeGapSMS)).cast("long")
    case "max_time_interval_sms" => org.apache.spark.sql.functions.max(col(TimeGapSMS)).cast("long")

    case "mean_time_interval_voice" => org.apache.spark.sql.functions.mean(col(TimeGapCall)).cast("double")
    case "min_time_interval_voice" => min(col(TimeGapCall)).cast("long")
    case "max_time_interval_voice" => max(col(TimeGapCall)).cast("long")

    case "mean_time_interval_gprs" => org.apache.spark.sql.functions.mean(col(TimeGapGPRS)).cast("double")
    case "min_time_interval_gprs" => min(col(TimeGapGPRS)).cast("long")
    case "max_time_interval_gprs" => max(col(TimeGapCall)).cast("long")

    case "sms_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(SMSCount)))
    case "voice_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(VoiceCount)))
    case "call_duration_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(CallDuration)))
    case "gprs_usag_sum_weekend" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(GprsUsage)))
    case "ratio_weekend_sms" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(SMSCount))) /
        sum(col(SMSCount))
    case "ratio_weekend_voice" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(VoiceCount))) /
        sum(col(VoiceCount))
    case "ratio_weekend_call_duration" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(CallDuration))) /
        sum(col(CallDuration))
    case "ratio_weekend_gprs_usage" =>
      sum(when(col("weekday_or_weekend") === "weekend", col(GprsUsage))) /
        sum(col(GprsUsage))

  }


  override def listNeedBeforeTransform: Seq[String] = Seq(dateKey, SMSCount, CallDuration, GprsUsage)

  override def listProducedBeforeTransform: Seq[(String, Column)] = {
    val w = Window.partitionBy(nidHash, month_index).orderBy(dateKey)
    val lagsms = when(col(SMSCount) > lit(0), max(when(col(SMSCount) > lit(0), col(dateKey))).over(w))
    val lagcall = when(col(CallDuration) > lit(0), max(when(col(CallDuration) > lit(0), col(dateKey))).over(w))
    val laggprs = when(col(GprsUsage) > lit(0), max(when(col(GprsUsage) > lit(0), col(dateKey))).over(w))
    val weekday = dayofweek(col(dateKey))
    val weekdayOrWeekend = when(col("weekday") === 7 || col("weekday") === 1, "weekend").otherwise("weekday")

    Seq(TimeGapSMS -> ((unix_timestamp(col(dateKey)) - unix_timestamp(lagsms))),
      TimeGapCall -> ((unix_timestamp(col(dateKey)) - unix_timestamp(lagcall))),
      TimeGapGPRS -> ((unix_timestamp(col(dateKey)) - unix_timestamp(laggprs))),
      "weekday" -> weekday,
      "weekday_or_weekend" -> weekdayOrWeekend
    )
  }
}