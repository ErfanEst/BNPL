package transform

import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.ml.util.{DefaultParamsReadable, Identifiable}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._

object BankInfo extends DefaultParamsReadable[BankInfo] {
  def apply(): BankInfo = new BankInfo(Identifiable.randomUID("bank"))
}

class BankInfo(override val uid: String) extends AbstractAggregator {

  def aggregator(name: String): Column = name match {
    case "total_bank_count" => countDistinct("bank_name")
  }

  def listNeedBeforeTransform: Seq[String] = Seq("fake_msisdn", "bank_name")


  def listProducedBeforeTransform: Seq[(String, Column)] = {
    Seq()
  }
}
