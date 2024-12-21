package transform

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object HandsetPrice extends DefaultParamsReadable[HandsetPrice] {
  def apply(): HandsetPrice = new HandsetPrice(Identifiable.randomUID("handsetFeature"))
}

class HandsetPrice(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "handset_onehot_vector" => count("cnt_of_days")
  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  // List of columns produced after transformation
  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }



}
