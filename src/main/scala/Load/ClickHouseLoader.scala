package Load

import org.apache.spark.sql.{DataFrame, SaveMode}
import core.Core.{appConfig, logger}
import utils.TableCreation

import scala.util.Try

object ClickHouseLoader {

  // ---- Feature table metadata & creation hooks ------------------------------

  sealed trait FeatureTable {
    /** base table name without index suffix */
    def base: String
    /** ensures the table exists for the given index */
    def ensure(index: Int): Unit
    final def fullName(index: Int): String = s"${base}_$index"
  }

  private object FeatureTable {
    case object CDR                  extends FeatureTable { val base = "CDR_features";                   def ensure(i: Int): Unit = TableCreation.createCDRFeaturesTable(i) }
    case object Recharge             extends FeatureTable { val base = "recharge_features";              def ensure(i: Int): Unit = TableCreation.createRechargeFeaturesTable(i) }
    case object CreditManagement     extends FeatureTable { val base = "credit_management_features";     def ensure(i: Int): Unit = TableCreation.createCreditManagementFeaturesTable(i) }
    case object UserInfo             extends FeatureTable { val base = "userinfo_features";              def ensure(i: Int): Unit = TableCreation.createUserInfoFeaturesTable(i) }
    case object DomesticTravel       extends FeatureTable { val base = "domestic_travel_features";       def ensure(i: Int): Unit = TableCreation.createDomesticTravelFeaturesTable(i) }
    case object LoanRec              extends FeatureTable { val base = "loanrec_features";               def ensure(i: Int): Unit = TableCreation.createLoanRecFeaturesTable(i) }
    case object LoanAssign           extends FeatureTable { val base = "loanassign_features";            def ensure(i: Int): Unit = TableCreation.createLoanAssignFeaturesTable(i) }
    case object PackagePurchaseExtra extends FeatureTable { val base = "package_purchase_extras_features";def ensure(i: Int): Unit = TableCreation.createPackagePurchaseExtrasTable(i) }
    case object PackagePurchase      extends FeatureTable { val base = "package_purchase_features";      def ensure(i: Int): Unit = TableCreation.createPackagePurchaseFeaturesTable(i) }
    case object PostPaid             extends FeatureTable { val base = "postpaid_features";              def ensure(i: Int): Unit = TableCreation.createPostPaidFeaturesTable(i) }
    case object BankInfo             extends FeatureTable { val base = "bankinfo_features";              def ensure(i: Int): Unit = TableCreation.createBankInfoFeaturesTable(i) }
    case object HandsetPrice         extends FeatureTable { val base = "handset_price_features";         def ensure(i: Int): Unit = TableCreation.createHandsetPriceFeaturesTable(i) }
    case object Package              extends FeatureTable { val base = "package_features";               def ensure(i: Int): Unit = TableCreation.createPackageFeaturesTable(i) }
    case object Arpu                 extends FeatureTable { val base = "arpu_features";                  def ensure(i: Int): Unit = TableCreation.createArpuFeaturesTable(i) }
  }

  // ---- JDBC option plumbing (centralized) -----------------------------------

  private val baseJdbcOptions: Map[String, String] = Map(
    "driver"   -> "com.clickhouse.jdbc.ClickHouseDriver",
    "url"      -> appConfig.getString("clickhouse.url"),
    "user"     -> appConfig.getString("clickhouse.user"),
    "password" -> appConfig.getString("clickhouse.password")
    // Some drivers honor these; harmless if ignored:
    // "rewriteBatchedStatements" -> "true",
    // "socket_timeout" -> "600000",
    // "connect_timeout" -> "60000"
  )

  // Limit the number of concurrent JDBC writers (optional; helps ClickHouse)
  // Configure in application.conf: clickhouse.jdbc.maxPartitions = 8
  private val maxPartitions: Int =
    Try(appConfig.getInt("clickhouse.jdbc.maxPartitions")).getOrElse(0) // 0 = don’t touch

  // ---- Single generic writer -------------------------------------------------

  def load(df: DataFrame, table: FeatureTable, index: Int, mode: SaveMode = SaveMode.Append): Unit = {
    // Ensure table exists
    table.ensure(index)

    // Control writer parallelism if requested
    val toWrite =
      if (maxPartitions > 0 && df.rdd.getNumPartitions > maxPartitions) df.coalesce(maxPartitions)
      else df

    val tableName = table.fullName(index)

    // Write once with shared options
    toWrite.write
      .format("jdbc")
      .options(baseJdbcOptions + ("dbtable" -> tableName))
      .mode(mode)
      .save()

    logger.info(s"Wrote $tableName to ClickHouse successfully.")
  }

  // ---- Compatibility shims (optional) ---------------------------------------

  import FeatureTable._
  def loadCDRData(df: DataFrame, index: Int): Unit                  = load(df, CDR, index)
  def loadRechargeData(df: DataFrame, index: Int): Unit             = load(df, Recharge, index)
  def loadCreditManagementData(df: DataFrame, index: Int): Unit     = load(df, CreditManagement, index)
  def loadUserInfoData(df: DataFrame, index: Int): Unit             = load(df, UserInfo, index)
  def loadDomesticTravelData(df: DataFrame, index: Int): Unit       = load(df, DomesticTravel, index)
  def loadLoanRecData(df: DataFrame, index: Int): Unit              = load(df, LoanRec, index)
  def loadLoanAssignData(df: DataFrame, index: Int): Unit           = load(df, LoanAssign, index)
  def loadPackagePurchaseExtrasData(df: DataFrame, index: Int): Unit= load(df, PackagePurchaseExtra, index)
  def loadPackagePurchaseData(df: DataFrame, index: Int): Unit      = load(df, PackagePurchase, index)
  def loadPostPaidFeaturesData(df: DataFrame, index: Int): Unit     = load(df, PostPaid, index)
  def loadBankInfoFeaturesData(df: DataFrame, index: Int): Unit     = load(df, BankInfo, index)
  def loadHandsetPriceFeaturesData(df: DataFrame, index: Int): Unit = load(df, HandsetPrice, index)
  def loadPackageFeaturesData(df: DataFrame, index: Int): Unit      = load(df, Package, index)
  def loadArpuFeaturesData(df: DataFrame, index: Int): Unit         = load(df, Arpu, index)
}
