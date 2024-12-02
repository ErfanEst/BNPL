package transform

import core.Core.SourceCol.Package.{ActivationDate, DeactivationDate, OfferAmount, OfferingCode, OfferingName}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, countDistinct, lit, lower, max, mean, min, regexp_extract, regexp_replace, sum, when}

object UserInfo extends DefaultParamsReadable[UserInfo] {
  def apply(): UserInfo = new UserInfo(Identifiable.randomUID("agg"))
}

class UserInfo(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "count_postpaid" => count(when(col("cleaned_account_type") === "postpaid", 1))
    case "count_prepaid" => count(when(col("cleaned_account_type") === "prepaid", 1))
    case "max_bib_age" => max("normalized_bib_age")
    case "min_bib_age" => min("normalized_bib_age")
    case "mean_bib_age" => mean("normalized_bib_age")
    case "mean_account_balance" => mean("normalized_account_balance")
    case "max_account_balance" => max("normalized_account_balance")
    case "min_account_balance" => min("normalized_account_balance")
  }

  def listNeedBeforeTransform: Seq[String] = Seq("contract_type_v", "bib_id", "account_balance")

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(
      "cleaned_account_type" -> when(col("contract_type_v") === "postpaid", "postpaid")
        .when(col("contract_type_v") === "prepaid", "prepaid")
        .otherwise("unknown"),
      "normalized_bib_age" -> when(col("bib_id").isNotNull, col("bib_id").cast("int"))
        .otherwise(lit(0)),
      "normalized_account_balance" -> when(col("account_balance").isNotNull, col("account_balance").cast("double"))
        .otherwise(lit(0))
    )
  }

}
