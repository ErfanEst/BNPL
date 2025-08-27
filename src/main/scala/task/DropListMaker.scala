package task

import org.rogach.scallop.{ScallopConf, ScallopOption}
import core.Core.{appConfig, logger, spark}
import utils.Utils.monthIndexOf
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.storage.StorageLevel

object DropListMaker {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    logger.info(s"Program started at: ${new java.util.Date(startTime)}")

    object Opts extends ScallopConf(args) {
      val date: ScallopOption[String] = opt[String](required = true, descr = "Date in the format yyyy-MM-dd")
      val name: ScallopOption[String] = opt[String](required = true, descr = "Name of the aggregation (config key)")
      verify()
    }

    val index          = monthIndexOf(Opts.date())
    val previousMonth  = index - 1
    val basePath       = appConfig.getString("basePath")
    val relPath        = appConfig.getString(Opts.name()) // e.g. "arpu.parquet" or a dir name
    val outputPrefix   = appConfig.getString("changeOwnershipPath") // e.g. ".../drop_list_"

    logger.info(s"index=$index previousMonth=$previousMonth name=${Opts.name()} relPath=$relPath")

    val df1Raw = spark.read.parquet(s"$basePath/$index/$relPath")
    val df2Raw = spark.read.parquet(s"$basePath/$previousMonth/$relPath")

    // Pick key column dynamically (require same key in both)
    val keyCol =
      if (df1Raw.columns.contains("fake_msisdn") && df2Raw.columns.contains("fake_msisdn")) "fake_msisdn"
      else if (df1Raw.columns.contains("bib_id") && df2Raw.columns.contains("bib_id")) "bib_id"
      else throw new IllegalArgumentException("Neither 'fake_msisdn' nor 'bib_id' present in BOTH DataFrames.")

    val icCol = "fake_ic_number"

    // Project only needed columns and drop nulls early (saves shuffle/memory)
    val keys1 = df1Raw.select(col(keyCol), col(icCol)).na.drop(Seq(keyCol, icCol))
    val keys2 = df2Raw.select(col(keyCol), col(icCol)).na.drop(Seq(keyCol, icCol))

    // Helper: keys with >1 distinct ic in a DF
    def explodingChanges(df: DataFrame): DataFrame = {
      val multi = df.groupBy(col(keyCol))
        .agg(countDistinct(col(icCol)).as("distinct_count"))
        .filter(col("distinct_count") > 1)
        .select(col(keyCol))

      multi.join(df, Seq(keyCol))
        .select(col(keyCol), col(icCol))
        .distinct()
    }

    val changes1 = explodingChanges(keys1)
    val changes2 = explodingChanges(keys2)

    // Small distinct ic sets reused → cache & broadcast
    val icFromChanges1 = changes1.select(col(icCol)).distinct().persist(StorageLevel.MEMORY_ONLY)
    val icFromDf1      = keys1.select(col(icCol)).distinct().persist(StorageLevel.MEMORY_ONLY)

    val finalChanges1 = keys2
      .join(broadcast(icFromChanges1), Seq(icCol))
      .select(col(keyCol), col(icCol))
      .distinct()

    val finalChanges2 = changes2
      .join(broadcast(icFromDf1), Seq(icCol), "left_anti")
      .select(col(keyCol), col(icCol))
      .distinct()

    val unionDF = finalChanges2.union(finalChanges1).distinct()

    // Optional: control file count; tune target number to your cluster/filesize goals
    val writers = math.min(8, unionDF.rdd.getNumPartitions) // e.g., cap at 8 part files
    val output  = s"$outputPrefix${previousMonth}_$index"

    unionDF.coalesce(writers).write.mode(SaveMode.Overwrite).parquet(output)

    icFromChanges1.unpersist(blocking = false)
    icFromDf1.unpersist(blocking = false)

    val duration = (System.currentTimeMillis() - startTime) / 1000
    logger.info(s"DropListMaker finished in $duration seconds. Wrote: $output")
    spark.stop()
  }
}
