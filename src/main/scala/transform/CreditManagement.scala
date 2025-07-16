package transform

import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.expressions.Window
import utils.Utils.CommonColumns.month_index

object CreditManagement extends DefaultParamsReadable[CreditManagement] {
  def apply(): CreditManagement = new CreditManagement(Identifiable.randomUID("credit_management"))
}

class CreditManagement(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {

    case "cnt_delayed" =>
      count(
        when(
          col("debt_status_1") === 4,
          col("installment_id")
        )
      )
    case "cnt_ontime" =>
      count(
        when(
          col("debt_status_1") === 3,
          col("installment_id")
        )
      )
    case "cnt_overdue" =>
      count(
        when(
          col("debt_status_1") === 2,
          col("installment_id")
        )
      )
    case "cnt_notdue" =>
      count(
        when(
          col("debt_status_1") === 1,
          col("installment_id")
        )
      )
    case "cnt_delayed_2" =>
      count(
        when(
          col("debt_status_2") === 4,
          col("installment_id")
        )
      )
    case "cnt_ontime_2" =>
      count(
        when(
          col("debt_status_2") === 3,
          col("installment_id")
        )
      )
    case "cnt_overdue_2" =>
      count(
        when(
          col("debt_status_2") === 2,
          col("installment_id")
        )
      )
    case "cnt_notdue_2" =>
      count(
        when(
          col("debt_status_2") === 1,
          col("installment_id")
        )
      )

    case "avg_days_delayed" => coalesce(mean(when(col("debt_status_1") === 4, col("days_delayed"))), lit(-1))
    case "avg_days_ontime" => coalesce(mean(when(col("debt_status_1") === 3, col("days_delayed"))), lit(1))
    case "avg_days_delayed_2" => coalesce(mean(when(col("debt_status_2") === 4, col("days_delayed"))), lit(-1))
    case "avg_days_ontime_2" => coalesce(mean(when(col("debt_status_2") === 3, col("days_delayed"))), lit(1))

    case "cnt_much_delayed_paid" => count(when(col("terrible_debt_status") === 2, col("installment_id")))
    case "cnt_much_delayed_notpaid" => count(when(col("terrible_debt_status") === 1, col("installment_id")))

  }

  def listNeedBeforeTransform: Seq[String] = Seq()

  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }
}
