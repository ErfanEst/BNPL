package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, max, mean, min, sum, when}

object PackagePurchase extends DefaultParamsReadable[PackagePurchase] {
  def apply(): PackagePurchase = new PackagePurchase(Identifiable.randomUID("agg"))
}

class PackagePurchase(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

    case "min_service_cnt" => min("cnt")
    case "max_service_cnt" => max("cnt")
    case "sum_service_cnt" => sum("cnt")
    case "min_service_amount" => min("amount")
    case "max_service_amount" => max("amount")
    case "sum_service_amount" => sum("amount")
    case "avg_Recharge_Money" => mean(when(col("service_type") === "Recharge Money", col("amount") / col("cnt")).otherwise(0))
    case "avg_DATA_BOLTON" => mean(when(col("service_type") === "DATA_BOLTON", col("amount") / col("cnt")).otherwise(0))
    case "avg_EREFILL" => mean(when(col("service_type") === "EREFILL", col("amount") / col("cnt")).otherwise(0))
    case "avg_DATA_BUYABLE" => mean(when(col("service_type") === "DATA_BUYABLE", col("amount") / col("cnt")).otherwise(0))
    case "avg_BillPayment" => mean(when(col("service_type") === "BillPayment", col("amount") / col("cnt")).otherwise(0))
    case "avg_Pay_Bill" => mean(when(col("service_type") === "Pay Bill", col("amount") / col("cnt")).otherwise(0))
    case "avg_TDD_BOLTON" => mean(when(col("service_type") === "TDD_BOLTON", col("amount") / col("cnt")).otherwise(0))
    case "avg_OTHER" => mean(when(
      col("service_type").isNotNull &&
        col("service_type").isin("Recharge Money", "DATA_BOLTON", "EREFILL", "DATA_BUYABLE", "BillPayment", "Pay Bill", "TDD_BOLTON") === false,
      col("amount") / col("cnt")
    ).otherwise(0))

  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }
}
