package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, max, min, sum, mean,  when}

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
    case "avg_Recharge_Money" => mean(when(col("service_type") === "Recharge Money", col("amount") / col("cnt")))
    case "avg_DATA_BOLTON" => mean(when(col("service_type") === "DATA_BOLTON", col("amount") / col("cnt")))
    case "avg_EREFILL" => mean(when(col("service_type") === "EREFILL", col("amount") / col("cnt")))
    case "avg_DATA_BUYABLE" => mean(when(col("service_type") === "DATA_BUYABLE", col("amount") / col("cnt")))
    case "avg_BillPayment" => mean(when(col("service_type") === "BillPayment", col("amount") / col("cnt")))
    case "avg_Pay_Bill" => mean(when(col("service_type") === "Pay Bill", col("amount") / col("cnt")))
    case "avg_TDD_BOLTON" => mean(when(col("service_type") === "TDD_BOLTON", col("amount") / col("cnt")))
    case "service_v[0]" => max(when(col("service_type") === "Recharge Money", 1).otherwise(0))
    case "service_v[1]" => max(when(col("service_type") === "DATA_BOLTON", 1).otherwise(0))
    case "service_v[2]" => max(when(col("service_type") === "EREFILL", 1).otherwise(0))
    case "service_v[3]" => max(when(col("service_type") === "DATA_BUYABLE", 1).otherwise(0))
    case "service_v[4]" => max(when(col("service_type") === "BillPayment", 1).otherwise(0))
    case "service_v[5]" => max(when(col("service_type") === "Pay Bill", 1).otherwise(0))
    case "service_v[6]" => max(when(col("service_type") === "TDD_BOLTON", 1).otherwise(0))
  }

  def listNeedBeforeTransform: Seq[String] = Seq("cnt", "amount", "service_type")

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq(
      "amount_per_cnt" -> (col("amount") / col("cnt")),
      "binary_service_presence" -> when(col("service_type").isNotNull, 1).otherwise(0)
    )
  }
}
