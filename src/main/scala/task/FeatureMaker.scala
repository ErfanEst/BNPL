package task

import core.Core.{IndexedColumn, RangedCol, aggregationColsYaml, appConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.rogach.scallop._
import org.rogach.scallop.stringConverter
import transform.Aggregate.aggregate
import utils.Utils.CommonColumns.nidHash
import utils.Utils.monthIndexOf


object FeatureMaker {

  var index = 0

  def main(args: Array[String]): Unit = {

    val startTime = System.currentTimeMillis()
    println(s"Program started at: ${new java.util.Date(startTime)}")

    val input_date = "2024-06-09"
    val backward = 1
    index = monthIndexOf(input_date)

    val indices = index until index - backward by -1
    val name = "Package"

    val outputColumns = reverseMapOfList(aggregationColsYaml.filter(_.name == name).map(_.features).flatMap(_.toList).toMap)
    println(outputColumns)

    val aggregatedDataFrames: Iterable[DataFrame] =
      aggregate(name = name, indices = indices, outputColumns = outputColumns, index = index)

    // Perform a full outer join on nid_hash for all DataFrames
    val combinedDataFrame = aggregatedDataFrames.reduce { (df1, df2) =>
      df1.join(df2, Seq(nidHash), "full_outer")
    }

    // Show the combined DataFrame (optional)
    combinedDataFrame.show(15)

    val result = fillNullValue(combinedDataFrame)

//     Write the combined DataFrame to a Parquet file
    result.write.mode("overwrite").parquet(s"/home/erfan/parquet_test/scala_features/${name}Test")


//    aggregate(name = "Package", indices = indices, outputColumns = outputColumns, index = index).foreach{
//      df => df.show(10)
//    }
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

  def fillNullValue(source : DataFrame): DataFrame = {
    val zero_list: List[String] = List(
      "mean_bib_age_1",
      "mean_account_balance_1",
      "count_postpaid_1",
      "count_prepaid_1",
      "max_account_balance_1",
      "min_account_balance_1",
      "max_bib_age_1",
      "min_bib_age_1"
    )

    val indexedZeroList = zero_list.zipWithIndex.map { case (value, num) => s"${index}_${value}"}

    val neg1_list: List[String]  = List()

    val sec1month_list: List[String]  = List()

    val sec3month_list: List[String]  = List()

    val oneMonthSeconds = 24*3600*30
    val threeMonthSeconds = 24*3600*30*3

    val finalSource = source.na.fill(0, source.columns.intersect(indexedZeroList))

    println("the indexedZeroList is : ")
    println(indexedZeroList)

    println("the final source is : ")
    println(finalSource.columns.mkString("Array(", ", ", ")"))

    finalSource
  }


}
