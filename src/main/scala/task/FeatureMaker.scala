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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")

        utils.ClickHouseLoader.loadHandsetPriceFeaturesData(finalDF)
        logger.info("PackagePurchase data loaded to ClickHouse with config defaults")


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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.bank_info_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")
        utils.ClickHouseLoader.loadBankInfoFeaturesData(finalDF)
        logger.info("BankInfo data loaded to ClickHouse with config defaults")


      case "PackagePurchase" | "PackagePurchaseExtras" =>

        val featureGroup = name match {
          case "PackagePurchase"       => "package_purchase_features"
          case "PackagePurchaseExtras" => "package_purchase_extras_features"
          case _ =>
            throw new IllegalArgumentException(s"Unknown feature group: $name")
        }

        // Step 1 — Aggregate for two months
        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)
        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly $name DFs")

        // Step 2 — Join in-memory
        val joinedDF = aggregatedDataFrames.reduce(_.join(_, Seq("fake_msisdn"), "outer"))
        val repartitioned = joinedDF.repartition(144, col("fake_msisdn")).cache()
        repartitioned.count()  // trigger cache
        logger.info("Join complete")
        logger.info(s"Joined RDD lineage:${repartitioned.rdd.toDebugString}")

        // Step 3 — Fill missing values using defaults from config
        // TRY: get the config; if missing, throw, so you never get a silent error or half-filled data
        val featureDefaultsConfig = try {
          appConfig.getConfig(s"featureDefaults.$featureGroup")
        } catch {
          case ex: com.typesafe.config.ConfigException.Missing =>
            throw new RuntimeException(
              s"Missing featureDefaults.$featureGroup in application.conf: " +
                s"please add under 'featureDefaults' node"
            )
        }

        // Typesafe Config .entrySet is Java, so convert to Scala Map[String, Any]
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
        logger.info(s"FinalDF RDD lineage:${finalDF.rdd.toDebugString}")

        // Step 4 — Final write to ClickHouse
        name match {
          case "PackagePurchase" =>
            utils.ClickHouseLoader.loadPackagePurchaseData(finalDF)
            logger.info("PackagePurchase data loaded to ClickHouse with config defaults")
          case "PackagePurchaseExtras" =>
            utils.ClickHouseLoader.loadPackagePurchaseExtrasData(finalDF)
            logger.info("PackagePurchaseExtras data loaded to ClickHouse with config defaults")
          case _ =>
            throw new IllegalArgumentException(s"Unknown feature group: $name")
        }


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



      case "Recharge" =>
        val outputColumns = reverseMapOfList(
          aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap
        )

        // Step 1 — Aggregate for two months
        val aggregatedDataFrames: Seq[DataFrame] =
          aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

        logger.info(s"Aggregated ${aggregatedDataFrames.size} monthly Recharge DFs")

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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"Recharge task completed: Final output written to $outPath")

        // Load to ClickHouse with config defaults
        utils.ClickHouseLoader.loadRechargeData(finalDF)
        logger.info("Recharge data loaded to ClickHouse with config defaults")


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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loanassign_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")
        utils.ClickHouseLoader.loadLoanAssignData(finalDF)
        logger.info("LoanAssign data loaded to ClickHouse with config defaults")

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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.loanrec_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")

        utils.ClickHouseLoader.loadLoanRecData(finalDF)
        logger.info("LoanRec data loaded to ClickHouse with config defaults")

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
        utils.ClickHouseLoader.loadCDRData(finalDF)
        logger.info("CDR data loaded to ClickHouse with config defaults")

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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.credit_management_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")
        // Load to ClickHouse with config defaults
        utils.ClickHouseLoader.loadCreditManagementData(finalDF)
        logger.info("Credit Management data loaded to ClickHouse with config defaults")

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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.user_info_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")
        utils.ClickHouseLoader.loadUserInfoData(finalDF)
        logger.info("User Info data loaded to ClickHouse with config defaults")


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
        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.domestic_travel_features")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")

        utils.ClickHouseLoader.loadDomesticTravelData(finalDF)
        logger.info("Domestic Travel data loaded to ClickHouse with config defaults")

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

        val featureDefaultsConfig = appConfig.getConfig("featureDefaults.post-paid-credit")
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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")

        utils.ClickHouseLoader.loadPostPaidFeaturesData(finalDF)
        logger.info("PostPaid data loaded to ClickHouse with config defaults")

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
//        val outPath = s"${appConfig.getString("outputPath")}/${name}_features_${index}_index/"
//        finalDF.write.mode("overwrite").parquet(outPath)
//        logger.info(s"${name} task completed: Final output written to $outPath")
        utils.ClickHouseLoader.loadPackageFeaturesData(finalDF)
        logger.info("Package data loaded to ClickHouse with config defaults")


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


