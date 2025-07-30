package task

import core.Core.{Conf, aggregationColsYaml, appConfig, logger, spark}
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}
import transform.Aggregate.aggregate
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.monthIndexOf
import org.apache.hadoop.fs.{FileSystem, Path}


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


        val tmpBaseDir = appConfig.getString("outputPath")
        val runStamp   = System.currentTimeMillis()
        val tmpDir     = s"$tmpBaseDir/_tmp_cdr_${index}_$runStamp"
        val fs         = FileSystem.get(spark.sparkContext.hadoopConfiguration)
        val tmpPath    = new Path(tmpDir)

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Saving ${aggregatedDataFrames.size} monthly CreditManagement DFs to $tmpDir")

        if (!fs.exists(tmpPath)) {
          fs.mkdirs(tmpPath)
          logger.info(s"Created temporary output directory: $tmpDir")
        }

//        println("point 0")
//        Thread.sleep(3000)

        aggregatedDataFrames.zipWithIndex.foreach { case (df, i) =>
          val path = s"$tmpDir/month_$i"
          df.write.mode("overwrite").parquet(path)
          logger.info(s"Saved month_$i to $path")
        }

//        println("point 1")
//        Thread.sleep(3000)

        // Step 4 — Read and fully cache with materialization
        val df1 = spark.read.parquet(s"$tmpDir/month_0")
        val df2 = spark.read.parquet(s"$tmpDir/month_1")

//        println("point 2")
//        Thread.sleep(3000)

        df1.count()  // fully materialize
        df2.count()  // fully materialize

//        println("point 3")
//        Thread.sleep(3000)

        logger.info(s"df1 RDD lineage:\n${df1.rdd.toDebugString}")
        logger.info(s"df2 RDD lineage:\n${df2.rdd.toDebugString}")

//        println("point 4")
//        Thread.sleep(3000)

        // Step 5 — Safe to delete temporary directory after caching
//        try {
//          if (fs.exists(tmpPath)) {
//            fs.delete(tmpPath, true)
//            logger.info(s"Deleted temporary directory: $tmpDir")
//          }
//        } catch {
//          case ex: Throwable =>
//            logger.warn(s"Failed to delete $tmpDir. You may need to clean it manually.", ex)
//        }

//        println("point 5")
//        Thread.sleep(3000)

        // Step 6 — Join the cached data
        val joined = df1.join(df2, Seq(bibID), "outer")
        joined.count()  // light action to materialize joined plan
        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${joined.rdd.toDebugString}")

//        println("point 6")
//        Thread.sleep(3000)

        val combinedDataFrame = joined.repartition(128, col(bibID))
        combinedDataFrame.count()  // optional light action

//        println("point 7")
//        Thread.sleep(3000)

        // Step 7 — Fill missing values using default values
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.cdr_features")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

//        println("point 8")
//        Thread.sleep(3000)

        var finalDF = combinedDataFrame
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

//        println("point 9")
//        Thread.sleep(3000)

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

//        println("point 10")
//        Thread.sleep(3000)

        // Load to ClickHouse with config defaults
        utils.CDRClickHouseLoader.loadCDRData(finalDF, index)
        logger.info("CDR data loaded to ClickHouse with config defaults")

      case "CreditManagement" =>
        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        println("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
          df1.join(df2, Seq("fake_msisdn"), "full_outer")
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

    val durationMillis = System.currentTimeMillis() - startTime
    val durationSec = durationMillis / 1000.0

    println(s"The code duration is: $durationSec seconds.")

    try {
//      metrics.MetricsPusher.push(durationSec, succeeded = true)
    } catch {
      case e: Exception =>
        println("Failed to push Prometheus metrics: " + e.getMessage)
    }

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


