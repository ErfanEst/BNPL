package task

import com.typesafe.config.ConfigFactory
import org.rogach.scallop.{ScallopConf, ScallopOption}
import utils.TableCreation
import core.Core.logger

import java.sql.DriverManager
import scala.collection.JavaConverters._

object FeatureStoreBuilder {

  // buffer to collect query-like defaults for CTE
  private val cteExpressions = scala.collection.mutable.ListBuffer.empty[String]

  private def safeColName(alias: String, colName: String): String = {
    val escapedCol = colName.replace("`", "``")
    s"$alias.`$escapedCol`"
  }

  def buildExpr(tableAlias: String, col: String, default: String, index: Long): String = {

    println(tableAlias, col, default, index)
    Thread.sleep(250)

    val safeCol = s"`$col`"

    if (default == null || default.trim.isEmpty) {
      s"$tableAlias.$safeCol"
    } else if (default.trim.toUpperCase == "NULL") {
      s"COALESCE($tableAlias.$safeCol, NULL)"
    } else if (default.trim.matches("""^-?\d+(\.\d+)?$""")) {
      s"COALESCE($tableAlias.$safeCol, $default)"
    } else if (default.trim.toUpperCase.startsWith("SELECT")) {
      val queryWithIndex = default.replace("<index>", index.toString)

      val cteName = s"default_${tableAlias}_$col"
        .replaceAll("""[^a-zA-Z0-9]""", "_")

      // Register this subquery as a CTE
      cteExpressions += s"$cteName AS ($queryWithIndex)"

      s"COALESCE($tableAlias.$safeCol, $cteName)"
    } else {
      s"COALESCE($tableAlias.$safeCol, $default)"
    }
  }

  def buildExprList(tableAlias: String, cols: Seq[String], defaults: Map[String, String], index: Long): Seq[String] = {
    cols.map { col =>
      val defaultVal = defaults.getOrElse(col, null)
      buildExpr(tableAlias, col, defaultVal, index)
    }
  }


  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    println(s"Program started at: ${new java.util.Date(startTime)}")

    object Opts extends ScallopConf(args) {
      val index: ScallopOption[Int] = opt[Int](required = true, descr = "Starting index")
      verify()
    }
    val index = Opts.index()
    val indices = index to (index - 1) by -1
    println(s"Index: $index")
    println(s"Indices: ${indices.mkString(", ")}")

    val config = ConfigFactory.load()
    val url = config.getString("clickhouse.url")
    val user = config.getString("clickhouse.user")
    val pass = config.getString("clickhouse.password")

    def getDefaults(table: String): Map[String, String] = {
      val path = s"featureDefaults.$table"
      if (!config.hasPath(path)) Map.empty
      else config.getConfig(path).entrySet().asScala.map(e => e.getKey -> e.getValue.unwrapped.toString).toMap
    }

    // Column lists for each table
    val cdrCols = List(
      "gprs_usage_sum_1","sms_count_1","ratio_weekend_gprs_usage_1","max_time_interval_sms_1",
      "mean_time_interval_voice_1","mean_time_interval_sms_1","min_time_interval_gprs_1",
      "mean_time_interval_gprs_1","ratio_weekend_call_duration_sum_1","gprs_usage_activedays_1",
      "min_time_interval_voice_1","call_duration_sum_1","voice_count_1","max_time_interval_voice_1",
      "max_time_interval_gprs_1","sms_activedays_1","min_time_interval_sms_1","ratio_weekend_sms_count_1",
      "voice_activedays_1","ratio_weekend_voice_count_1","gprs_usage_sum_2","sms_count_2",
      "ratio_weekend_gprs_usage_2","max_time_interval_sms_2","mean_time_interval_voice_2",
      "mean_time_interval_sms_2","min_time_interval_gprs_2","mean_time_interval_gprs_2",
      "ratio_weekend_call_duration_sum_2","gprs_usage_activedays_2","min_time_interval_voice_2",
      "call_duration_sum_2","voice_count_2","max_time_interval_voice_2","max_time_interval_gprs_2",
      "sms_activedays_2","min_time_interval_sms_2","ratio_weekend_sms_count_2","voice_activedays_2",
      "ratio_weekend_voice_count_2"
    )

    val rechargeCols = List(
      "count_recharge_1","ratio_afternoon_recharge_1","max_recharge_1","min_recharge_1","mean_recharge_1",
      "sum_recharge_1","balance_max_1","mean_balance_before_recharge_1","ratio_weekend_recharge_1",
      "count_recharge_2","ratio_afternoon_recharge_2","max_recharge_2","min_recharge_2","mean_recharge_2",
      "sum_recharge_2","balance_max_2","mean_balance_before_recharge_2","ratio_weekend_recharge_2"
    )

    val creditCols = List(
      "avg_days_ontime_1","avg_days_delayed_1","cnt_delayed_1","cnt_notdue_1","cnt_ontime_1",
      "cnt_overdue_1","cnt_much_delayed_notpaid_2","cnt_ontime_2_2","cnt_much_delayed_paid_2",
      "avg_days_ontime_2_2","cnt_notdue_2_2","avg_days_delayed_2_2","cnt_delayed_2_2","cnt_overdue_2_2"
    )

    val userinfoCols = List(
      "abstat_HARD_1","abstat_ERASED_1","abstat_OTHER_1","abstat_READY TO ACTIVATE SOFT_1","postpaid_1",
      "max_account_balance_1","abstat_SOFT_1","bib_age_1","abstat_ACTIVE_1","abstat_READY TO ACTIVE_1"
    )

    val travelCols = List(
      "avg_daily_travel_1","unique_travel_days_1","travel_1_5_1","max_travel_in_day_1",
      "travel_11_plus_1","travel_6_10_1","total_travel_1","avg_daily_travel_2","unique_travel_days_2",
      "travel_1_5_2","max_travel_in_day_2","travel_11_plus_2","travel_6_10_2","total_travel_2"
    )

    val loanRecCols = List("mean_time_to_repay_1","mean_time_to_repay_2")

    val loanAssignCols = List(
      "loan_amount_max_1","count_loans_1","loan_amount_min_1","max_time_interval_loan_1",
      "loan_amount_sum_1","min_time_interval_loan_1","mean_time_interval_loan_1",
      "loan_amount_max_2","count_loans_2","loan_amount_min_2","max_time_interval_loan_2",
      "loan_amount_sum_2","min_time_interval_loan_2","mean_time_interval_loan_2"
    )

    val pkgExtrasCols = List(
      "CNT_INDIRECT_PURCHASE_1","service_v[BillPayment]_1","service_v[Pay_Bill]_1","service_v[EREFILL]_1",
      "service_v[OTHER]_1","service_v[DATA_BOLTON]_1","service_v[Recharge_Money]_1","service_v[TDD_BOLTON]_1",
      "service_v[DATA_BUYABLE]_1","CNT_BUNDLE_PURCHASE_1","CNT_DIRECT_PURCHASE_1",
      "CNT_INDIRECT_PURCHASE_2","service_v[BillPayment]_2","service_v[Pay_Bill]_2","service_v[EREFILL]_2",
      "service_v[OTHER]_2","service_v[DATA_BOLTON]_2","service_v[Recharge_Money]_2",
      "service_v[TDD_BOLTON]_2","service_v[DATA_BUYABLE]_2","CNT_BUNDLE_PURCHASE_2","CNT_DIRECT_PURCHASE_2"
    )

    val pkgPurchaseCols = List(
      "avg_DATA_BUYABLE_1","sum_service_cnt_1","avg_DATA_BOLTON_1","avg_Pay_Bill_1","avg_Recharge_Money_1",
      "min_service_cnt_1","avg_TDD_BOLTON_1","avg_EREFILL_1","avg_BillPayment_1","sum_service_amount_1",
      "max_service_cnt_1","max_service_amount_1","min_service_amount_1","avg_OTHER_1",
      "avg_DATA_BUYABLE_2","sum_service_cnt_2","avg_DATA_BOLTON_2","avg_Pay_Bill_2","avg_Recharge_Money_2",
      "min_service_cnt_2","avg_TDD_BOLTON_2","avg_EREFILL_2","avg_BillPayment_2","sum_service_amount_2",
      "max_service_cnt_2","max_service_amount_2","min_service_amount_2","avg_OTHER_2"
    )

    val postpaidCols = List(
      "is_creditor_1","unbilled_ratio_1","account_status_active_1","has_payment_1","credit_ratio_1",
      "is_suspended_1","over_limit_flag_1","deposit_to_credit_ratio_1","churn_1","days_since_last_payment_1",
      "avl_credit_limit_growth_2","deposit_change_2","total_credit_utilization_growth_2",
      "credit_limit_change_2","credit_limit_growth_rate_2"
    )

    val bankinfoCols = List(
      "bank_active_days_count_1","bank_loyalty_ratio_first_1","total_bank_count_1","month_end_sms_ratio_1",
      "avg_daily_bank_sms_first_1","bank_active_days_count_2","total_bank_count_2",
      "bank_loyalty_ratio_both_2","month_end_sms_ratio_2","avg_daily_bank_sms_both_2"
    )

    val handsetCols = List(
      "xiaomi_usage_ratio_1","handset_v[4]_1","max_days_single_handset_1","handset_stability_1",
      "handset_v[0]_1","handset_v[1]_1","handset_v[2]_1","avg_days_per_handset_1","apple_usage_ratio_1",
      "total_unique_handsets_1","samsung_usage_ratio_1","total_unique_brands_1","handset_v[3]_1",
      "total_usage_days_1","brand_diversity_ratio_1","huawei_usage_ratio_1","xiaomi_usage_ratio_2",
      "handset_v[4]_2","max_days_single_handset_2","handset_stability_2","handset_v[0]_2","handset_v[1]_2",
      "handset_v[2]_2","avg_days_per_handset_2","apple_usage_ratio_2","total_unique_handsets_2",
      "samsung_usage_ratio_2","total_unique_brands_2","handset_v[3]_2","total_usage_days_2",
      "brand_diversity_ratio_2","huawei_usage_ratio_2"
    )

    val packageCols = List(
      "mean_package_period_1","ratio_offeramount_zero_1","max_package_period_1","sum_offer_amount_1",
      "count_packages_1","min_package_period_1","sum_data_MB_1","count_distinct_offername_1",
      "count_distinct_offercode_1","mean_package_period_2","ratio_offeramount_zero_2","max_package_period_2",
      "sum_offer_amount_2","count_packages_2","min_package_period_2","sum_data_MB_2",
      "count_distinct_offername_2","count_distinct_offercode_2"
    )

    val arpuCols = List(
      "sms_revenue_first_1","subscription_revenue_first_1","age_1","voice_revenue_first_1","contract_type_1",
      "gprs_revenue_first_1","prepaid_to_postpaid_1","res_com_score_first_1","gender_1","site__large_city_2",
      "gprs_revenue_change_2","site__USO_2","site__Port_2","site__Oil Platform_2","voice_revenue_change_2",
      "subscription_revenue_second_2","voice_revenue_second_2","site__Industrial Area_2",
      "site__Touristic Area_2","site__Island_2","sms_revenue_change_2","site__Airport_2",
      "sms_revenue_second_2","gprs_revenue_second_2","site__road_village_2","site__University_2"
    )

    // Get defaults for each
    val defaultsMap = Map(
      "cdr" -> getDefaults("cdr_features"),
      "recharge" -> getDefaults("recharge_features"),
      "credit" -> getDefaults("credit_management_features"),
      "userinfo" -> getDefaults("user_info_features"),
      "travel" -> getDefaults("domestic_travel_features"),
      "loanrec" -> getDefaults("loanrec_features"),
      "loanassign" -> getDefaults("loanassign_features"),
      "pkgextras" -> getDefaults("package_purchase_extras_features"),
      "pkgpurchase" -> getDefaults("package_purchase_features"),
      "postpaid" -> getDefaults("post-paid-credit-2"),
      "bankinfo" -> getDefaults("bank_info_features"),
      "handset" -> getDefaults("handset_price"),
      "package" -> getDefaults("package_features"),
      "arpu" -> getDefaults("arpu-2")
    )


    val selectColumns =
      (List("ids.bib_id") ++
        buildExprList("cdr", cdrCols, defaultsMap("cdr"), index.toLong) ++
        buildExprList("r", rechargeCols, defaultsMap("recharge"), index.toLong) ++
        buildExprList("c", creditCols, defaultsMap("credit"), index.toLong) ++
        buildExprList("u", userinfoCols, defaultsMap("userinfo"), index.toLong) ++
        buildExprList("t", travelCols, defaultsMap("travel"), index.toLong) ++
        buildExprList("lr", loanRecCols, defaultsMap("loanrec"), index.toLong) ++
        buildExprList("la", loanAssignCols, defaultsMap("loanassign"), index.toLong) ++
        buildExprList("pe", pkgExtrasCols, defaultsMap("pkgextras"), index.toLong) ++
        buildExprList("pp", pkgPurchaseCols, defaultsMap("pkgpurchase"), index.toLong) ++
        buildExprList("po", postpaidCols, defaultsMap("postpaid"), index.toLong) ++
        buildExprList("b", bankinfoCols, defaultsMap("bankinfo"), index.toLong) ++
        buildExprList("h", handsetCols, defaultsMap("handset"), index.toLong) ++
        buildExprList("pa", packageCols, defaultsMap("package"), index.toLong) ++
        buildExprList("a", arpuCols, defaultsMap("arpu"), index.toLong)
        ).mkString(",")

    val createAllIdsTableSQL =
      s"""
         |CREATE TABLE IF NOT EXISTS all_bib_ids_$index ENGINE = MergeTree() ORDER BY bib_id AS
         |SELECT bib_id FROM CDR_features_$index
         |UNION DISTINCT SELECT bib_id FROM recharge_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM credit_management_features_$index
         |UNION DISTINCT SELECT bib_id FROM userinfo_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM domestic_travel_features_$index
         |UNION DISTINCT SELECT bib_id FROM loanrec_features_$index
         |UNION DISTINCT SELECT bib_id FROM loanassign_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM package_purchase_extras_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM package_purchase_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM postpaid_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM bankinfo_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM handset_price_features_$index
         |UNION DISTINCT SELECT bib_id FROM package_features_$index
         |UNION DISTINCT SELECT fake_msisdn AS bib_id FROM arpu_features_$index
         |""".stripMargin

    val withPart =
      if (cteExpressions.nonEmpty)
        cteExpressions.mkString("WITH ", ", ", " ")
      else
        ""

    val insertSQL =
      s"""
         |$withPart
         |INSERT INTO feature_store_$index
         |SELECT
         |  $selectColumns
         |FROM all_bib_ids_$index ids
         |LEFT JOIN CDR_features_$index cdr ON ids.bib_id = cdr.bib_id
         |LEFT JOIN recharge_features_$index r ON ids.bib_id = r.bib_id
         |LEFT JOIN credit_management_features_$index c ON ids.bib_id = c.fake_msisdn
         |LEFT JOIN userinfo_features_$index u ON ids.bib_id = u.bib_id
         |LEFT JOIN domestic_travel_features_$index t ON ids.bib_id = t.fake_msisdn
         |LEFT JOIN loanrec_features_$index lr ON ids.bib_id = lr.bib_id
         |LEFT JOIN loanassign_features_$index la ON ids.bib_id = la.bib_id
         |LEFT JOIN package_purchase_extras_features_$index pe ON ids.bib_id = pe.fake_msisdn
         |LEFT JOIN package_purchase_features_$index pp ON ids.bib_id = pp.fake_msisdn
         |LEFT JOIN postpaid_features_$index po ON ids.bib_id = po.fake_msisdn
         |LEFT JOIN bankinfo_features_$index b ON ids.bib_id = b.fake_msisdn
         |LEFT JOIN handset_price_features_$index h ON ids.bib_id = h.fake_msisdn
         |LEFT JOIN package_features_$index pa ON ids.bib_id = pa.bib_id
         |LEFT JOIN arpu_features_$index a ON ids.bib_id = a.fake_msisdn
         |SETTINGS join_use_nulls = 1
         |""".stripMargin

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, pass)
    try {
      val stmt = conn.createStatement()
      TableCreation.createFeatureStoreTable(index)
      stmt.execute(createAllIdsTableSQL)
      stmt.execute(insertSQL)
      logger.info(s"feature_store_$index populated.")
    } finally {
      conn.close()
    }

    println(s"Program completed at: ${new java.util.Date(System.currentTimeMillis())}")
    println(s"Total execution time: ${(System.currentTimeMillis() - startTime) / 1000.0} seconds")
  }
}
