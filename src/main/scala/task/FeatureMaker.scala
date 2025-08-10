package task

import core.Core.{Conf, aggregationColsYaml, appConfig, logger, spark}
import org.apache.spark.sql.{DataFrame, SparkSession}
import transform.Aggregate.aggregate
import org.apache.spark.sql.functions._
import utils.Utils.CommonColumns.{bibID, month_index, nidHash}
import utils.Utils.monthIndexOf
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.jdk.CollectionConverters.asScalaSetConverter

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
        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.handset_price")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "BankInfo" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.bank_info")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "PackagePurchase" | "PackagePurchaseExtras" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.package_purchase_extras")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "Arpu" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count() // Trigger cache

        repartitioned.createOrReplaceTempView("finalDF_view")
        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill nulls with defaults from config
        val featureDefaultsConfig = appConfig.getConfig(s"featureDefaults.${name.toLowerCase}")

        // ✅ Proper way to extract key-value pairs from Typesafe Config
        val featureDefaults: Map[String, AnyRef] = featureDefaultsConfig.root().entrySet().asScala.map { entry =>
          val key = entry.getKey
          val value = entry.getValue.unwrapped() // extract raw value (String, Int, etc.)
          key -> value
        }.toMap

        // Log extracted keys
        logger.info(s"Extracted default keys from config: ${featureDefaults.keys.mkString(", ")}")

        // Apply default values to DataFrame
        var finalDF = repartitioned

        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            val valueToFill: Option[Any] = defaultVal match {
              case query: String if query.trim.toLowerCase.startsWith("select") =>
                try {
                  logger.info(s"Running query to fill '$colName': $query")
                  val queryResult = spark.sql(query)
                  val value = queryResult.first().get(0)
                  logger.info(s"Query result for '$colName' = $value")
                  Some(value)
                } catch {
                  case e: Exception =>
                    logger.warn(s"Failed to run query for column '$colName': $query", e)
                    None
                }
              case _ =>
                logger.info(s"Filling nulls in '$colName' with static default: $defaultVal")
                Some(defaultVal)
            }

            valueToFill.foreach { v =>
              finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(v)))
            }
          } else {
            logger.warn(s"Column '$colName' not found in DataFrame; skipping default fill")
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")


        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")


      case "LoanAssign" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loan_assign")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "LoanRec" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loan_rec")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")


      //        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
      //        val aggregatedDataFrames: Seq[DataFrame] =
      //          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)
      //
      //        println("The data frame was created successfully...")
      //
      //        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
      //          df1.join(df2, Seq(bibID), "full_outer")
      //        }
      //
      //        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
      //        println("Task finished successfully.")

      case "CDR" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.cdr_features")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "CreditManagement" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly CreditManagement DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.credit_management")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "UserInfo" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.user_info")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")


      case "DomesticTravel" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.domestic_travel")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "PostPaid" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache

        // Register the repartitioned DataFrame as a temp view for SQL queries from config
        repartitioned.createOrReplaceTempView("finalDF_view")

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.postpaid")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {

            val valueToFill: Option[Any] = defaultVal match {
              case query: String if query.trim.toLowerCase.startsWith("select") =>
                try {
                  val queryResult = spark.sql(query)
                  val value = queryResult.first().get(0)
                  Some(value)
                } catch {
                  case e: Exception =>
                    logger.warn(s"Failed to run query for column '$colName': $query", e)
                    None
                }
              case _ => Some(defaultVal)
            }

            valueToFill.foreach { v =>
              finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(v)))
            }
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "Package" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.package_features")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case "Recharge" =>

        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly ${name} DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq(bibID), "outer"))
        val repartitioned = joinedDF.repartition(144, col(bibID)).cache()
        repartitioned.count()  // trigger cache

        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:\n${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.recharge_features")
        val featureDefaults: Map[String, Any] = featureDefaultsConfig.entrySet().toArray
          .map(_.toString.split("=")(0).trim)
          .map(k => k -> featureDefaultsConfig.getAnyRef(k))
          .toMap

        var finalDF = repartitioned
        featureDefaults.foreach { case (colName, defaultVal) =>
          if (finalDF.columns.contains(colName)) {
            finalDF = finalDF.withColumn(colName, coalesce(col(colName), lit(defaultVal)))
          }
        }

        logger.info("Default value filling complete")
        logger.info(s"FinalDF RDD lineage:\n${finalDF.rdd.toDebugString}")

        // Step 4 — Final write
        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
        finalDF.write.mode("overwrite").parquet(outPath)
        logger.info(s"${name} task completed: Final output written to $outPath")

      case _ =>

        val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)

        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info("The data frame was created successfully...")

        val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>

          df1.join(df2, Seq("fake_msisdn"), "full_outer")
        }

        combinedDataFrame.write.mode("overwrite").parquet(appConfig.getString("outputPath") + s"/${name}_features_${index}_index/")
        logger.info("Task finished successfully.")

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

