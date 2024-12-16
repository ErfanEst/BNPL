package task

import core.Core.{Conf, aggregationColsYaml, appConfig}
import org.apache.spark.sql.DataFrame
import transform.Aggregate.aggregate
import utils.Utils.CommonColumns.nidHash
import utils.Utils.monthIndexOf

object FeatureMaker {

  var index: Int = _

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    println(s"Program started at: ${new java.util.Date(startTime)}")

//    val opts = new Conf(args)
    // 2024-09-05
    index = monthIndexOf("2024-09-05")
    val indices = index until index - 1 by - 1
    val name = "CDR"

//    println(s"date is: ${opts.date()}\nname is: ${opts.name}")

    name match {
      case _ =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
            aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq(nidHash), "full_outer")
        }
        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")
    }

    val duration = System.currentTimeMillis() - startTime
    println(s"The code duration is: ${duration/1000} seconds.")
  }

  private def reverseMapOfList(a_map: Map[String, List[Int]]): Map[Int, List[String]] = {
    a_map.toList.flatMap(x => x._2.map((_, x._1)))
      .foldLeft(Map[Int, List[String]]()) {
        (z, f) =>
          if (z.contains(f._1)) {
            z + (f._1 -> (z(f._1) :+ f._2))
          } else {
            z + (f._1 -> List(f._2))
          }
      }
  }

}


