package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, max, min, sum, mean,  when}

object PackagePurchaseExtras extends DefaultParamsReadable[PackagePurchaseExtras] {
  def apply(): PackagePurchaseExtras = new PackagePurchaseExtras(Identifiable.randomUID("agg"))
}

class PackagePurchaseExtras(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "CNT_BUNDLE_PURCHASE" => sum(when(col("source_system_cd") === "BUNDLE_PURCHASE", 1).otherwise(0))
    case "CNT_DIRECT_PURCHASE" => sum(when(col("source_system_cd") === "DIRECT_PURCHASE", 1).otherwise(0))
    case "CNT_INDIRECT_PURCHASE" => sum(when(col("source_system_cd") === "INDIRECT_PURCHASE", 1).otherwise(0))
    case "service_v[Recharge_Money]" => max(when(col("service_type") === "Recharge Money", col("cnt")).otherwise(0))
    case "service_v[DATA_BOLTON]" => max(when(col("service_type") === "DATA_BOLTON", col("cnt")).otherwise(0))
    case "service_v[EREFILL]" => max(when(col("service_type") === "EREFILL", col("cnt")).otherwise(0))
    case "service_v[DATA_BUYABLE]" => max(when(col("service_type") === "DATA_BUYABLE", col("cnt")).otherwise(0))
    case "service_v[BillPayment]" => max(when(col("service_type") === "BillPayment", col("cnt")).otherwise(0))
    case "service_v[Pay_Bill]" => max(when(col("service_type") === "Pay Bill", col("cnt")).otherwise(0))
    case "service_v[TDD_BOLTON]" => max(when(col("service_type") === "TDD_BOLTON", col("cnt")).otherwise(0))
    case "service_v[OTHER]" => max(when(
      col("service_type").isNotNull &&
        col("service_type").isin("Recharge Money", "DATA_BOLTON", "EREFILL", "DATA_BUYABLE", "BillPayment", "Pay Bill", "TDD_BOLTON") === false, col("cnt")).otherwise(0))
  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {

    Seq(
//      "amount_per_cnt" -> (col("amount") / col("cnt")),
//      "binary_service_presence" -> when(col("service_type").isNotNull, 1).otherwise(0)
    )
  }
}
