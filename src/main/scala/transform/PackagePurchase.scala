package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, max, min, sum, mean,  when}

object PackagePurchase extends DefaultParamsReadable[PackagePurchase] {
  def apply(): PackagePurchase = new PackagePurchase(Identifiable.randomUID("agg"))
}

class PackagePurchase(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "min_service_cnt_1" => min("cnt")
    case "max_service_cnt_1" => max("cnt")
    case "sum_service_cnt_1" => sum("cnt")
    case "min_service_amount_1" => min("amount")
    case "max_service_amount_1" => max("amount")
    case "sum_service_amount_1" => sum("amount")
    case "avg_Recharge_Money_1" => mean(when(col("service_type") === "Recharge Money", col("amount") / col("cnt")))
    case "avg_DATA_BOLTON_1" => mean(when(col("service_type") === "DATA_BOLTON", col("amount") / col("cnt")))
    case "avg_EREFILL_1" => mean(when(col("service_type") === "EREFILL", col("amount") / col("cnt")))
    case "avg_DATA_BUYABLE_1" => mean(when(col("service_type") === "DATA_BUYABLE", col("amount") / col("cnt")))
    case "avg_BillPayment_1" => mean(when(col("service_type") === "BillPayment", col("amount") / col("cnt")))
    case "avg_Pay_Bill_1" => mean(when(col("service_type") === "Pay Bill", col("amount") / col("cnt")))
    case "avg_TDD_BOLTON_1" => mean(when(col("service_type") === "TDD_BOLTON", col("amount") / col("cnt")))
    case "binary_Recharge_Money" => max(when(col("service_type") === "Recharge Money", 1).otherwise(0))
    case "binary_DATA_BOLTON" => max(when(col("service_type") === "DATA_BOLTON", 1).otherwise(0))
    case "binary_EREFILL" => max(when(col("service_type") === "EREFILL", 1).otherwise(0))
    case "binary_DATA_BUYABLE" => max(when(col("service_type") === "DATA_BUYABLE", 1).otherwise(0))
    case "binary_BillPayment" => max(when(col("service_type") === "BillPayment", 1).otherwise(0))
    case "binary_Pay_Bill" => max(when(col("service_type") === "Pay Bill", 1).otherwise(0))
    case "binary_TDD_BOLTON" => max(when(col("service_type") === "TDD_BOLTON", 1).otherwise(0))
  }

  def listNeedBeforeTransform: Seq[String] = Seq("cnt", "amount", "service_type")

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(
      "amount_per_cnt" -> (col("amount") / col("cnt")),
      "binary_service_presence" -> when(col("service_type").isNotNull, 1).otherwise(0)
    )
  }
}
