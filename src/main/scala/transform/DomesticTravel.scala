package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index

object DomesticTravel extends DefaultParamsReadable[DomesticTravel] {
  def apply(): DomesticTravel = new DomesticTravel(Identifiable.randomUID("handsetFeature"))
}

class DomesticTravel(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

    case "unique_travel_days" => countDistinct("date_key")
    case "total_travel" => sum("sum_travel")
    case "avg_daily_travel" => avg("sum_travel")
    case "max_travel_in_day" => max("sum_travel")
    case "travel_1_5" => first(when(col("travel_bucket") === "1-5", lit(1)).otherwise(0))
    case "travel_6_10" => first(when(col("travel_bucket") === "6-10", lit(1)).otherwise(0))
    case "travel_11_plus" => first(when(col("travel_bucket") === "11+", lit(1)).otherwise(0))

  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy("fake_msisdn")

    Seq(
      "total_travel" -> sum("sum_travel").over(w),
      "travel_bucket" -> when(col("total_travel") <= 5, "1-5")
                        .when(col("total_travel") > 5 && col("total_travel") <= 10, "6-10")
                        .otherwise("11+")
    )
  }
}
