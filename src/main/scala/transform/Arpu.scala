package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index
import utils.Utils.arpuDetails.large_city
import utils.Utils.arpuDetails.road_village

object Arpu extends DefaultParamsReadable[Arpu] {
  def apply(): Arpu = new Arpu(Identifiable.randomUID("agg"))
}

class Arpu(override val uid: String) extends AbstractAggregator {
  def aggregator(name: String): Column = name match {

    case "age"                            => max("age")

    case "res_com_score_first"            => last(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("dense_rank") === 1), col("res_com_score")).otherwise(0))
    case "voice_revenue_first"            => sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("voice_revenue")))
    case "gprs_revenue_first"             => sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("gprs_revenue")))
    case "sms_revenue_first"              => sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("sms_revenue")))
    case "subscription_revenue_first"     => sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("subscription_revenue")))

    case "res_com_score_second"           => sum(when(col("dense_rank") === 2, col("res_com_score")).otherwise(0))
    case "voice_revenue_second"           => sum(when((col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("voice_revenue")).otherwise(0))
    case "gprs_revenue_second"            => sum(when((col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("gprs_revenue")).otherwise(0))
    case "sms_revenue_second"             => sum(when((col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("sms_revenue")).otherwise(0))
    case "subscription_revenue_second"    => sum(when((col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("subscription_revenue")).otherwise(0))

    case "gender"                  => last(when(col("gender") === "F", 1).otherwise(0))

    case "prepaid_to_postpaid"     => first(when(col("cnt_contract_type") === 2, 1).otherwise(0))
    case "contract_type"           => last(when(col("cnt_contract_type") === 2, 1).otherwise(when(col("contract_type") === "N", 1).otherwise(0)))

    case "site__road_village"      => first("site__road_village")
    case "site__large_city"        => first("site__large_city")

    case "site__Airport"        => coalesce(first(when(col("site_type") === "Airport", 1).otherwise(0)), lit(0))
    case "site__Industrial Area" => coalesce(first(when(col("site_type") === "Industrial Area", 1).otherwise(0)), lit(0))
    case "site__Island"         => coalesce(first(when(col("site_type") === "Island", 1).otherwise(0)), lit(0))
    case "site__Oil Platform"   => coalesce(first(when(col("site_type") === "Oil Platform", 1).otherwise(0)), lit(0))
    case "site__Port"           => coalesce(first(when(col("site_type") === "Port", 1).otherwise(0)), lit(0))
    case "site__Touristic Area" => coalesce(first(when(col("site_type") === "Touristic Area", 1).otherwise(0)), lit(0))
    case "site__USO"            => coalesce(first(when(col("site_type") === "USO", 1).otherwise(0)), lit(0))
    case "site__University"     => coalesce(first(when(col("site_type") === "University", 1).otherwise(0)), lit(0))

    case "res_com_score_change"    => coalesce(first(col("res_com_score_1")/col("res_com_score_2") - 1.0), lit(0.0))
    case "sms_revenue_change"      => coalesce(first(col("sms_revenue_1")/col("sms_revenue_2") - 1.0), lit(0.0))
    case "gprs_revenue_change"     => coalesce(first(col("gprs_revenue_1")/col("gprs_revenue_2") - 1.0), lit(0.0))
    case "voice_revenue_change"    => coalesce(first(col("voice_revenue_1")/col("voice_revenue_2") - 1.0), lit(0.0))

  }

  def listNeedBeforeTransform: Seq[String] = Seq("site_type", "dense_rank")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy("fake_msisdn").orderBy(month_index)

    Seq(

      "cnt_contract_type" -> size(array_distinct(collect_list(col("contract_type")).over(w))),

      "res_com_score_1" -> last(when((col("dense_rank") === 2 && col("count_dense_rank")  > 1) || (col("dense_rank") === 1 && col("count_dense_rank") === 1), col("res_com_score")), ignoreNulls = true).over(w),
      "sms_revenue_1" -> sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("sms_revenue"))).over(w),
      "gprs_revenue_1" -> sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("gprs_revenue"))).over(w),
      "voice_revenue_1" -> sum(when((col("dense_rank") === 2 &&  col("count_dense_rank")  > 1) || (col("dense_rank") === 1 &&  col("count_dense_rank") === 1), col("voice_revenue"))).over(w),

      "res_com_score_2" -> last(when((col("dense_rank") === 1 && col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("res_com_score")), ignoreNulls = true).over(w),
      "sms_revenue_2" -> sum(when((col("dense_rank") === 1 && col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("sms_revenue")).otherwise(0)).over(w),
      "gprs_revenue_2" -> sum(when((col("dense_rank") === 1 && col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("gprs_revenue")).otherwise(0)).over(w),
      "voice_revenue_2" -> sum(when((col("dense_rank") === 1 && col("count_dense_rank")  > 1) || (col("dense_rank") === 2 && col("count_dense_rank") === 1), col("voice_revenue")).otherwise(0)).over(w),

      "r1" -> first("res_com_score_1", true).over(w),
      "r2" -> first("res_com_score_2", true).over(w),
      "v1" -> first("voice_revenue_1", true).over(w),
      "v2" -> first("voice_revenue_2", true).over(w),
      "g1" -> first("gprs_revenue_1", true).over(w),
      "g2" -> first("gprs_revenue_2", true).over(w),
      "s1" -> first("sms_revenue_1", true).over(w),
      "s2" -> first("sms_revenue_2", true).over(w),

      "site__road_village" -> when(last("site_type").over(w).isin(road_village.toSeq: _*), 1).otherwise(0),
      "site__large_city" -> when(last("site_type").over(w).isin(large_city.toSeq: _*), 1).otherwise(0)
    )
  }
}
