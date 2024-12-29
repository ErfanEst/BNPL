package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Arpu extends DefaultParamsReadable[Arpu] {
  def apply(): Arpu = new Arpu(Identifiable.randomUID("agg"))
}

class Arpu(override val uid: String) extends AbstractAggregator {
  def aggregator(name: String): Column = name match {
    case "age" => max("age")
    case "avg_res_com_score" => avg("res_com_score")
    case "avg_voice_revenue" => avg("voice_revenue")
    case "avg_gprs_revenue" => avg("gprs_revenue")
    case "avg_sms_revenue" => avg("sms_revenue")
    case "avg_subscription_revenue" => avg("subscription_revenue")
    case "count_active_fake_msisdn" => avg("fake_msisdn")
  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }
}
