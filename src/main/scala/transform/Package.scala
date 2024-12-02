package transform

import core.Core.SourceCol.Package.{ActivationDate, DeactivationDate, OfferAmount, OfferingCode, OfferingName}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

object Package extends DefaultParamsReadable[Package] {
  def apply(): Package = new Package(Identifiable.randomUID("agg"))
}

class Package(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "count_distinct_offercode" => countDistinct("newofferingCode")
    case "count_distinct_offername" => countDistinct("newofferingName")
    case "count_packages" => count("*")
    case "sum_data_MB" => sum("data_usage")
    case "sum_offer_amount" => sum("offer_amount")
    case "mean_package_period" => mean(col(DeactivationDate) - col(ActivationDate))
    case "max_package_period" => max(col(DeactivationDate) - col(ActivationDate))
    case "min_package_period" => min(col(DeactivationDate) - col(ActivationDate))
    case "ratio_offeramount_zero" => mean(when(col(OfferAmount) === 0, 1).otherwise(0))
  }

  def listNeedBeforeTransform: Seq[String] = Seq(OfferingCode, OfferingName)

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(
      "newofferingCode" -> regexp_replace(col(OfferingCode), lit(" "), lit("")),
      "newofferingName" -> regexp_replace(col(OfferingName), " ", ""),
      "onl_zip" -> lower(col("newofferingName")),
      "gb" -> when(col("onl_zip").isNotNull, regexp_extract(col("onl_zip"), "(\\d+(\\.\\d+)?)gb", 1).cast("double"))
        .otherwise(1024),
      "mb" -> when(col("onl_zip").isNotNull, regexp_extract(col("onl_zip"), "(\\d+(\\.\\d+)?)mb", 1).cast("double"))
        .otherwise(1024),
      "data_usage" -> ((col("gb") * 1024) + col("mb"))
    )
  }
}
