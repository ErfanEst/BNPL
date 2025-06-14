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

  object arpuDetails {
    val road_village: Seq[String] = Seq("Road", "Road - Village", "Village", "Multi Villages", "Small City", "Residential Area")
    val large_city: Seq[String] = Seq("Capital City", "Main City", "Middle City")
    val other_sites: Seq[String] = Seq("Airport", "Industrial Area", "Island", "Oil Platform", "Port", "Touristic Area", "USO", "University")
  }

  def getLeafNeededColumns(complex: Column): Seq[String] = {
    complex.expr.collectLeaves.map(_.toString).filter(_!="").filter(_.head == '\'').map(_.tail)
  }

}
