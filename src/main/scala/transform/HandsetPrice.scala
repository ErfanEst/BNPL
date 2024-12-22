package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector

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
  }

  def listNeedBeforeTransform: Seq[String] = Seq("handset_brand")

  // List of columns produced after transformation
  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }



}
