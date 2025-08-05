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

    case "samsung_usage_ratio" => sum(when(col("handset_brand") === "SAMSUNG",  col("cnt_of_days")))
    case "xiaomi_usage_ratio" => sum(when(col("handset_brand") === "XIAOMI",  col("cnt_of_days")))
    case "apple_usage_ratio" => sum(when(col("handset_brand") === "APPLE",  col("cnt_of_days")))
    case "huawei_usage_ratio" => sum(when(col("handset_brand") === "HUAWEI",  col("cnt_of_days")))


    case "handset_v[0]" => sum(when(getVectorElement(col("handsetVec"), lit(0)) > 0, 1).otherwise(0))
    case "handset_v[1]" => sum(when(getVectorElement(col("handsetVec"), lit(1)) > 0, 1).otherwise(0))
    case "handset_v[2]" => sum(when(getVectorElement(col("handsetVec"), lit(2)) > 0, 1).otherwise(0))
    case "handset_v[3]" => sum(when(getVectorElement(col("handsetVec"), lit(3)) > 0, 1).otherwise(0))
    case "handset_v[4]" => sum(when(getVectorElement(col("handsetVec"), lit(4)) > 0, 1).otherwise(0))

    case "total_unique_handsets" => size(collect_set("handset_model"))
    case "total_unique_brands" => size(collect_set("handset_brand"))

    case "brand_diversity_ratio" => size(collect_set("handset_model"))/size(collect_set("handset_brand"))

    case "total_usage_days" => sum("cnt_of_days")
    
    case "max_days_single_handset" => max("sum_cnt_of_days")

    case "handset_stability" => max("sum_cnt_of_days")/sum("sum_cnt_of_days")

    case "avg_days_per_handset" => sum("cnt_of_days")/size(collect_set("handset_model"))

  }

  def listNeedBeforeTransform: Seq[String] = Seq("handset_model", "handset_brand", "handsetVec", "handset_brand", "cnt_of_days")

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    Seq(
      "sum_cnt_of_days" -> col("sum_cnt_of_days"),
    )

  }
}
