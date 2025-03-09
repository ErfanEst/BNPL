package transform

import core.Core.SourceCol.Package.{ActivationDate, DeactivationDate, OfferAmount, OfferingCode, OfferingName}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, count, countDistinct, current_date, lit, lower, max, mean, min, regexp_extract, regexp_replace, sum, to_date, when, year}

object UserInfo extends DefaultParamsReadable[UserInfo] {
  def apply(): UserInfo = new UserInfo(Identifiable.randomUID("agg"))
}

class UserInfo(override val uid: String) extends AbstractAggregator {

  override def aggregator(name: String): Column = name match {
    case "postpaid" => max(when(col("contract_type_v") === "N", lit(1)).otherwise(lit(0)))
//    case "bib_age" =>
    case "abstat_ACTIVE" => max(when(col("ability_status") === "ACTIVE", lit(1)).otherwise(lit(0)))
    case "abstat_ERASED" => max(when(col("ability_status") === "ERASED", lit(1)).otherwise(lit(0)))
    case "abstat_HARD" => max(when(col("ability_status") === "HARD", lit(1)).otherwise(lit(0)))
    case "abstat_OTHER" => max(when(col("ability_status") === "OTHER", lit(1)).otherwise(lit(0)))
    case "abstat_READY TO ACTIVATE SOFT" => max(when(col("ability_status") === "READY TO ACTIVATE SOFT", lit(1)).otherwise(lit(0)))
    case "abstat_READY TO ACTIVE" => max(when(col("ability_status") === "READY TO ACTIVE", lit(1)).otherwise(lit(0)))
    case "abstat_SOFT" => max(when(col("ability_status") === "SOFT", lit(1)).otherwise(lit(0)))
    case "max_account_balance" => max("normalized_account_balance")
    case "bib_age" => max("normalized_bib_age")
  }

  override def listNeedBeforeTransform: Seq[String] = Seq(
    "contract_type_v",
    "registration_date_d",
    "account_balance"
  )

  override def listProducedBeforeTransform: Seq[(String, Column)] = {
    val currentYear = year(current_date())
    Seq(
      "cleaned_account_type" -> when(col("contract_type_v") === "N", "postpaid")
        .when(col("contract_type_v") === "P", "prepaid")
        .otherwise("unknown"),
      "registration_date" -> to_date(col("registration_date_d"), "yyyyMMdd"),
      "normalized_bib_age" -> (currentYear - year(col("registration_date"))),
      "normalized_account_balance" -> when(col("account_balance").isNotNull, col("account_balance").cast("double"))
        .otherwise(lit(0))
    )
  }
}
