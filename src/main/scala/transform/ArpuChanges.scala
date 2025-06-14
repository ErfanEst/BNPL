package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index
import utils.Utils.arpuDetails.large_city
import utils.Utils.arpuDetails.road_village

object ArpuChanges extends DefaultParamsReadable[ArpuChanges] {
  def apply(): ArpuChanges = new ArpuChanges(Identifiable.randomUID("agg"))
}

class ArpuChanges(override val uid: String) extends AbstractAggregator {
  def aggregator(name: String): Column = name match {

    case "res_com_score_change"    => last(when(col("dense_rank") === 1, col("res_com_score_1")/col("res_com_score_2")), ignoreNulls = true)
    case "sms_revenue_change"      => last(col("sms_revenue_1")/col("sms_revenue_2") - 1, ignoreNulls = true)
    case "gprs_revenue_change"     => last(col("gprs_revenue_1")/col("gprs_revenue_2") - 1, ignoreNulls = true)
    case "voice_revenue_change"    => last(when(col("dense_rank") === 2, col("voice_revenue_1"))/ when(col("voice_revenue_2") === 1, col("voice_revenue_2") -1))

  }

  def listNeedBeforeTransform: Seq[String] = Seq("dense_rank", "res_com_score", "sms_revenue", "gprs_revenue", "voice_revenue")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy("fake_msisdn", "dense_rank")

    Seq(

      "res_com_score_1" -> last(when(col("dense_rank") === 2 , col("res_com_score")), ignoreNulls = true).over(w),
      "sms_revenue_1" -> sum(when(col("dense_rank") === 2 , col("sms_revenue"))).over(w),
      "gprs_revenue_1" -> sum(when(col("dense_rank") === 2 , col("gprs_revenue"))).over(w),
      "voice_revenue_1" -> sum(when(col("dense_rank") === 2 , col("voice_revenue"))).over(w),

      "res_com_score_2" -> last(when(col("dense_rank") === 1 , col("res_com_score")), ignoreNulls = true).over(w),
      "sms_revenue_2" -> sum(when(col("dense_rank") === 1 , col("sms_revenue"))).over(w),
      "gprs_revenue_2" -> sum(when(col("dense_rank") === 1 , col("gprs_revenue"))).over(w),
      "voice_revenue_2" -> sum(when(col("dense_rank") === 1 , col("voice_revenue"))).over(w),

    )
  }
}
