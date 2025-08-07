package task

import com.typesafe.config.ConfigFactory
import utils.TableCreation

import java.sql.DriverManager
import scala.collection.JavaConverters._

object FeatureStoreBuilder {
  def main(args: Array[String]): Unit = {
    // 1. Load config
    val config = ConfigFactory.load()
    val clickhouseUrl = config.getString("clickhouse.url")
    val clickhouseUser = config.getString("clickhouse.user")
    val clickhousePassword = config.getString("clickhouse.password")

    // 2. Helper to read defaults for each feature table
    def getDefaults(table: String): Map[String, String] = {
      val path = s"featureDefaults.$table"
      if (!config.hasPath(path)) Map.empty
      else config.getConfig(path).entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped.toString).toMap
    }

    // 3. Read defaults from config
    val cdrDefaults      = getDefaults("cdr_features")
    val rechargeDefaults = getDefaults("recharge_features")
    val creditDefaults   = getDefaults("credit_management_features")

    // (Optional) Print loaded defaults for debugging
    println(s"CDR Defaults: $cdrDefaults")
    println(s"Recharge Defaults: $rechargeDefaults")
    println(s"Credit Management Defaults: $creditDefaults")

    // 4. List of columns for each feature table
    val cdrCols = List(
      "gprs_usage_sum_1", "sms_count_1", "ratio_weekend_gprs_usage_1", "max_time_interval_sms_1",
      "mean_time_interval_voice_1", "mean_time_interval_sms_1", "min_time_interval_gprs_1",
      "mean_time_interval_gprs_1", "ratio_weekend_call_duration_sum_1", "gprs_usage_activedays_1",
      "min_time_interval_voice_1", "call_duration_sum_1", "voice_count_1", "max_time_interval_voice_1",
      "max_time_interval_gprs_1", "sms_activedays_1", "min_time_interval_sms_1", "ratio_weekend_sms_count_1",
      "voice_activedays_1", "ratio_weekend_voice_count_1", "gprs_usage_sum_2", "sms_count_2",
      "ratio_weekend_gprs_usage_2", "max_time_interval_sms_2", "mean_time_interval_voice_2",
      "mean_time_interval_sms_2", "min_time_interval_gprs_2", "mean_time_interval_gprs_2",
      "ratio_weekend_call_duration_sum_2", "gprs_usage_activedays_2", "min_time_interval_voice_2",
      "call_duration_sum_2", "voice_count_2", "max_time_interval_voice_2", "max_time_interval_gprs_2",
      "sms_activedays_2", "min_time_interval_sms_2", "ratio_weekend_sms_count_2", "voice_activedays_2",
      "ratio_weekend_voice_count_2"
    )
    val rechargeCols = List(
      "count_recharge_1", "ratio_afternoon_recharge_1", "max_recharge_1", "min_recharge_1", "mean_recharge_1",
      "sum_recharge_1", "balance_max_1", "mean_balance_before_recharge_1", "ratio_weekend_recharge_1",
      "count_recharge_2", "ratio_afternoon_recharge_2", "max_recharge_2", "min_recharge_2", "mean_recharge_2",
      "sum_recharge_2", "balance_max_2", "mean_balance_before_recharge_2", "ratio_weekend_recharge_2"
    )
    val creditCols = List(
      "avg_days_ontime_1", "avg_days_delayed_1", "cnt_delayed_1", "cnt_notdue_1", "cnt_ontime_1",
      "cnt_overdue_1", "cnt_much_delayed_notpaid_2", "cnt_ontime_2_2", "cnt_much_delayed_paid_2",
      "avg_days_ontime_2_2", "cnt_notdue_2_2", "avg_days_delayed_2_2", "cnt_delayed_2_2", "cnt_overdue_2_2"
    )

    // 5. Helper to COALESCE columns with config defaults (quotes for strings)
    def buildExpr(tableAlias: String, cols: List[String], defaults: Map[String, String]): List[String] =
      cols.map { col =>
        defaults.get(col).map { v =>
          val needsQuotes = !v.matches("^-?\\d+(\\.\\d+)?$")
          val defVal = if (needsQuotes) s"'$v'" else v
          s"COALESCE($tableAlias.$col, $defVal) AS $col"
        }.getOrElse(s"$tableAlias.$col AS $col")
      }

    // 6. Compose full SELECT column list
    val selectColumns = (
      List("ids.bib_id") ++
        buildExpr("cdr", cdrCols, cdrDefaults) ++
        buildExpr("r", rechargeCols, rechargeDefaults) ++
        buildExpr("c", creditCols, creditDefaults)
      ).mkString(",")

    // 7. Final SQLs
    val createAllIdsTableSQL =
      """
        |CREATE TABLE IF NOT EXISTS all_bib_ids (
        |  bib_id String
        |) ENGINE = MergeTree()
        |ORDER BY bib_id
        |AS
        |SELECT bib_id FROM CDR_features
        |UNION DISTINCT
        |SELECT bib_id FROM recharge_features
        |UNION DISTINCT
        |SELECT fake_msisdn AS bib_id FROM credit_management_features
      """.stripMargin.trim

    val insertSQL =
      s"""
         |INSERT INTO feature_store
         |SELECT
         |  $selectColumns
         |FROM all_bib_ids AS ids
         |LEFT JOIN CDR_features AS cdr             ON ids.bib_id = cdr.bib_id
         |LEFT JOIN recharge_features AS r          ON ids.bib_id = r.bib_id
         |LEFT JOIN credit_management_features AS c ON ids.bib_id = c.fake_msisdn
         |SETTINGS join_use_nulls = 1
  """.stripMargin.trim

    // 8. Run table creation and insertion
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(clickhouseUrl, clickhouseUser, clickhousePassword)
    try {
      val stmt = conn.createStatement()

      // Ensure feature_store table exists
      TableCreation.createFeatureStoreTable()

      // Create the all_bib_ids union table
      stmt.execute(createAllIdsTableSQL)

      // Populate feature_store
      stmt.execute(insertSQL)
      println("feature_store has been populated with joined data and featureDefaults null-filling.")
    } finally {
      conn.close()
    }
  }
}