package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index

object HandsetPriceBrands extends DefaultParamsReadable[HandsetPriceBrands] {
  def apply(): HandsetPriceBrands = new HandsetPriceBrands(Identifiable.randomUID("handsetFeature"))
}

class HandsetPriceBrands(override val uid: String) extends AbstractAggregator {

  private val getVectorElement = udf((vector: Vector, index: Int) => {
    if (vector != null && index >= 0 && index < vector.size) {
      vector(index)
    } else {
      null.asInstanceOf[Double]
    }
  })

  def aggregator(name: String): Column = name match {

    case "samsung_usage_ratio" => sum(when(col("handset_brand") === "SAMSUNG", col("cnt_of_days")/col("total_usage_days")))
    case "xiaomi_usage_ratio" => max(when(col("handset_brand") === "XIAOMI", col("cnt_of_days")/col("total_usage_days")))
    case "apple_usage_ratio" => max(when(col("handset_brand") === "APPLE", col("cnt_of_days")/col("total_usage_days")))
    case "huawei_usage_ratio" => max(when(col("handset_brand") === "HUAWEI", col("cnt_of_days")/col("total_usage_days")))

  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    val w = Window.partitionBy("fake_msisdn", month_index)

    Seq(
      "total_usage_days" -> sum("cnt_of_days").over(w),
    )

  }
}
