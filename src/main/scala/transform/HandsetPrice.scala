package transform

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object HandsetPrice extends DefaultParamsReadable[HandsetPrice] {
  def apply(): HandsetPrice = new HandsetPrice(Identifiable.randomUID("handsetFeature"))
}

class HandsetPrice(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "handset_onehot_vector" =>
      expr("vector_to_array(handsetVec)")
  }

  def listNeedBeforeTransform: Seq[String] = Seq("fake_ic_number", "handset_brand")

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(
      "handset_names" -> collect_list(col("handset_brand_2"))
    )
  }

  val cvm = new CountVectorizerModel(Array("SAMSUNG", "XIAOMI", "HUAWEI", "APPLE", "Other"))
    .setInputCol("handset_names")
    .setOutputCol("handset_onehot_vector")

}
