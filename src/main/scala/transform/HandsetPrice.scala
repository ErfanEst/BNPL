package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index

object HandsetPrice extends DefaultParamsReadable[HandsetPrice] {
  def apply(): HandsetPrice = new HandsetPrice(Identifiable.randomUID("handsetFeature"))
}

class HandsetPrice(override val uid: String) extends AbstractAggregator {

  private val getVectorElement = udf((vector: Vector, index: Int) => {
    if (vector != null && index >= 0 && index < vector.size) {
      vector(index)
    } else {
      null.asInstanceOf[Double]
    }
  })

  def aggregator(name: String): Column = name match {

    case "handset_v[0]" => sum(when(getVectorElement(col("handsetVec"), lit(0)) > 0, 1).otherwise(0))
    case "handset_v[1]" => sum(when(getVectorElement(col("handsetVec"), lit(1)) > 0, 1).otherwise(0))
    case "handset_v[2]" => sum(when(getVectorElement(col("handsetVec"), lit(2)) > 0, 1).otherwise(0))
    case "handset_v[3]" => sum(when(getVectorElement(col("handsetVec"), lit(3)) > 0, 1).otherwise(0))
    case "handset_v[4]" => sum(when(getVectorElement(col("handsetVec"), lit(4)) > 0, 1).otherwise(0))

    case "sum_unique_handsets" => max("total_unique_handsets")
    case "sum_unique_brands" => max("total_unique_brands")

    case "brand_diversity_ratio" => max(col("total_unique_handsets")/col("total_unique_brands"))

    case "sum_usage_days" => sum("cnt_of_days")
    case "max_days_single_handset" => max("cnt_of_days")

    case "handset_stability" => first(col("max_days_single")/col("total_usage_days"))

    case "avg_days_per_handset" => max(col("total_usage_days")/col("total_unique_handsets"))

  }

  def listNeedBeforeTransform: Seq[String] = Seq("handset_model", "handset_brand", "handsetVec", "handset_brand", "cnt_of_days")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w2 = Window.partitionBy("fake_msisdn", month_index)

    Seq(
      "total_unique_handsets" -> size(collect_set("handset_model").over(w2)),
      "total_unique_brands" -> size(collect_set("handset_brand").over(w2)),
      "total_usage_days" -> sum("cnt_of_days").over(w2),
      "max_days_single" -> max("cnt_of_days").over(w2),
      "brand_days" -> sum("cnt_of_days").over(w2),
    )

  }
}
