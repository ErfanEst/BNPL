package task

import core.Core.{Conf, aggregationColsYaml, appConfig}
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}
import transform.Aggregate.aggregate
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.monthIndexOf

object FeatureMaker {

  var index: Int = _

  def main(args: Array[String]): Unit = {


    val startTime = System.currentTimeMillis()
    println(s"Program started at: ${new java.util.Date(startTime)}")

    object Opts extends ScallopConf(args) {
      val date: ScallopOption[String] = opt[String](required = true, descr = "Date in the format yyyy-MM-dd")
      val name: ScallopOption[String] = opt[String](required = true, descr = "Name of the aggregation")
      verify()
    }

    index = monthIndexOf(Opts.date())

    val indices = index until index - 1 by - 1
    val name = Opts.name()

    println(index, indices, name)

    name match {

      case "HandsetPrice" =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "HandsetPriceBrands" =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn", "handset_brand"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "BankInfo" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "PackagePurchase" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
            aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "Arpu" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "ArpuChanges" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        println(outputColumns)
        println(",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,")
        println(name)
        Thread.sleep(3000)

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "Recharge" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq(bibID), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")


      case "LoanAssign" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq(bibID), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")


      case "LoanRec" =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq(bibID), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case "CDR" =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq(bibID), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        println("Task finished successfully.")

      case _ =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>

          df1.join(df2, Seq("fake_msisdn"), "full_outer")
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


