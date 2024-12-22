package utils

import com.github.mfathi91.time.PersianDate
import org.apache.spark.sql.Column

import java.time.LocalDate

object Utils {

  def monthIndexOf(date: String): Int = {
    val gregorianDate = LocalDate.parse(date)
    val persianDate = PersianDate.fromGregorian(gregorianDate)

    if (persianDate.getDayOfMonth >= 20) {
      12 * persianDate.getYear + persianDate.getMonthValue + 1
    } else {
      12 * persianDate.getYear + persianDate.getMonthValue
    }
  }

  object CommonColumns {
    val bibID = "bib_id"
    val nidHash = "nid_hash"
    var month_index = "month_index"
    val contract_shift = "contract_shift"
    val feature_month_index = "feature_month_index"
    val dateKey = "date"
    val dateTime = "date_timestam"
  }

  def getLeafNeededColumns(complex: Column): Seq[String] = {
    complex.expr.collectLeaves.map(_.toString).filter(_!="").filter(_.head == '\'').map(_.tail)
  }

}
