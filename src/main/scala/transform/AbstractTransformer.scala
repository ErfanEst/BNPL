package transform

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.shared.HasOutputCols
import org.apache.spark.ml.param.{IntArrayParam, ParamMap}
import org.apache.spark.ml.util.DefaultParamsWritable
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class AbstractTransformer extends Transformer with HasOutputCols with DefaultParamsWritable {

  def listOfTransformers: Seq[Dataset[_] => DataFrame] = Seq(transform)
  /**
   * @return The name of the columns which are required in the source to do the transformation.
   */
  def getInputColumns: Seq[String]

  def selectTransform(name: String, dataset: Dataset[_]): DataFrame

  val _indices = new IntArrayParam(this, "month_indices", "")

//  def setMonthIndices(value: Seq[Int]): AbstractTransformer = set(_indices, value.toArray)
  def setMonthIndices(value: Seq[Int]): AbstractTransformer = set(_indices, value.toArray)
  /**
   * Sets the desired output columns.
   * @param value The desired output columns
   * @return The transformer itself.
   */
  def setOutputColumns(value: Array[String]): AbstractTransformer = set(outputCols, value)

  /* We assume that we do not usually need to concat the transformers.
     If it is not the case, the subclass should implement this method. */
  override def transformSchema(schema: StructType): StructType = StructType(Seq())
  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

}