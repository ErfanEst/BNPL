package utils


import java.sql.DriverManager
import core.Core.{appConfig, logger}

object TableCreation {
  def createCDRFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"CDR_features_${index}"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          bib_id String,

          -- 1-month window
          gprs_usage_sum_1 Float64,
          sms_count_1 Int64,
          ratio_weekend_gprs_usage_1 Float64,
          max_time_interval_sms_1 Int64,
          mean_time_interval_voice_1 Float64,
          mean_time_interval_sms_1 Float64,
          min_time_interval_gprs_1 Int64,
          mean_time_interval_gprs_1 Float64,
          ratio_weekend_call_duration_sum_1 Float64,
          gprs_usage_activedays_1 Int64,
          min_time_interval_voice_1 Int64,
          call_duration_sum_1 Float64,
          voice_count_1 Int64,
          max_time_interval_voice_1 Int64,
          max_time_interval_gprs_1 Int64,
          sms_activedays_1 Int64,
          min_time_interval_sms_1 Int64,
          ratio_weekend_sms_count_1 Float64,
          voice_activedays_1 Int64,
          ratio_weekend_voice_count_1 Float64,

          -- 2-month window
          gprs_usage_sum_2 Float64,
          sms_count_2 Int64,
          ratio_weekend_gprs_usage_2 Float64,
          max_time_interval_sms_2 Int64,
          mean_time_interval_voice_2 Float64,
          mean_time_interval_sms_2 Float64,
          min_time_interval_gprs_2 Int64,
          mean_time_interval_gprs_2 Float64,
          ratio_weekend_call_duration_sum_2 Float64,
          gprs_usage_activedays_2 Int64,
          min_time_interval_voice_2 Int64,
          call_duration_sum_2 Float64,
          voice_count_2 Int64,
          max_time_interval_voice_2 Int64,
          max_time_interval_gprs_2 Int64,
          sms_activedays_2 Int64,
          min_time_interval_sms_2 Int64,
          ratio_weekend_sms_count_2 Float64,
          voice_activedays_2 Int64,
          ratio_weekend_voice_count_2 Float64
        )
        ENGINE = MergeTree()
        ORDER BY (bib_id)
        """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }
  def createRechargeFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"recharge_features_${index}"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        bib_id String,
        count_recharge_1 Int64,
        ratio_afternoon_recharge_1 Float64,
        max_recharge_1 Float64,
        min_recharge_1 Float64,
        mean_recharge_1 Float64,
        sum_recharge_1 Float64,
        balance_max_1 Float64,
        mean_balance_before_recharge_1 Float64,
        ratio_weekend_recharge_1 Float64,
        count_recharge_2 Int64,
        ratio_afternoon_recharge_2 Float64,
        max_recharge_2 Float64,
        min_recharge_2 Float64,
        mean_recharge_2 Float64,
        sum_recharge_2 Float64,
        balance_max_2 Float64,
        mean_balance_before_recharge_2 Float64,
        ratio_weekend_recharge_2 Float64
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createCreditManagementFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"credit_management_features_${index}"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        avg_days_ontime_1 Float64,
        avg_days_delayed_1 Float64,
        cnt_delayed_1 Int64,
        cnt_notdue_1 Int64,
        cnt_ontime_1 Int64,
        cnt_overdue_1 Int64,
        cnt_much_delayed_notpaid_2 Int64,
        cnt_ontime_2_2 Int64,
        cnt_much_delayed_paid_2 Int64,
        avg_days_ontime_2_2 Float64,
        cnt_notdue_2_2 Int64,
        avg_days_delayed_2_2 Float64,
        cnt_delayed_2_2 Int64,
        cnt_overdue_2_2 Int64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }


  def createUserInfoFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"userinfo_features_${index}"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        bib_id String,
        `abstat_HARD_1` Int32,
        `abstat_ERASED_1` Int32,
        `abstat_OTHER_1` Int32,
        `abstat_READY TO ACTIVATE SOFT_1` Int32,
        `postpaid_1` Int32,
        `max_account_balance_1` Float64,
        `abstat_SOFT_1` Int32,
        `bib_age_1` Int32,
        `abstat_ACTIVE_1` Int32,
        `abstat_READY TO ACTIVE_1` Int32
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createDomesticTravelFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"domestic_travel_features_${index}"

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        avg_daily_travel_1 Float64,
        unique_travel_days_1 Int64,
        travel_1_5_1 Int32,
        max_travel_in_day_1 Int64,
        travel_11_plus_1 Int32,
        travel_6_10_1 Int32,
        total_travel_1 Int64,
        avg_daily_travel_2 Float64,
        unique_travel_days_2 Int64,
        travel_1_5_2 Int32,
        max_travel_in_day_2 Int64,
        travel_11_plus_2 Int32,
        travel_6_10_2 Int32,
        total_travel_2 Int64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createLoanRecFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"loanrec_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        bib_id String,
        `mean_time_to_repay_1` Float64,
        `mean_time_to_repay_2` Float64
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createLoanAssignFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"loanassign_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        bib_id String,
        `loan_amount_max_1` Int32,
        `count_loans_1` Int64,
        `loan_amount_min_1` Int32,
        `max_time_interval_loan_1` Int64,
        `loan_amount_sum_1` Int64,
        `min_time_interval_loan_1` Int64,
        `mean_time_interval_loan_1` Float64,
        `loan_amount_max_2` Int32,
        `count_loans_2` Int64,
        `loan_amount_min_2` Int32,
        `max_time_interval_loan_2` Int64,
        `loan_amount_sum_2` Int64,
        `min_time_interval_loan_2` Int64,
        `mean_time_interval_loan_2` Float64
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createPackagePurchaseExtrasTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"package_purchase_extras_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        CNT_INDIRECT_PURCHASE_1 Int64,
        `service_v[BillPayment]_1` Int64,
        `service_v[Pay_Bill]_1` Int64,
        `service_v[EREFILL]_1` Int64,
        `service_v[OTHER]_1` Int64,
        `service_v[DATA_BOLTON]_1` Int64,
        `service_v[Recharge_Money]_1` Int64,
        `service_v[TDD_BOLTON]_1` Int64,
        `service_v[DATA_BUYABLE]_1` Int64,
        CNT_BUNDLE_PURCHASE_1 Int64,
        CNT_DIRECT_PURCHASE_1 Int64,
        CNT_INDIRECT_PURCHASE_2 Int64,
        `service_v[BillPayment]_2` Int64,
        `service_v[Pay_Bill]_2` Int64,
        `service_v[EREFILL]_2` Int64,
        `service_v[OTHER]_2` Int64,
        `service_v[DATA_BOLTON]_2` Int64,
        `service_v[Recharge_Money]_2` Int64,
        `service_v[TDD_BOLTON]_2` Int64,
        `service_v[DATA_BUYABLE]_2` Int64,
        CNT_BUNDLE_PURCHASE_2 Int64,
        CNT_DIRECT_PURCHASE_2 Int64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }



  def createPackagePurchaseFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"package_purchase_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        avg_DATA_BUYABLE_1 Float64,
        sum_service_cnt_1 Int64,
        avg_DATA_BOLTON_1 Float64,
        avg_Pay_Bill_1 Float64,
        avg_Recharge_Money_1 Float64,
        min_service_cnt_1 Int64,
        avg_TDD_BOLTON_1 Float64,
        avg_EREFILL_1 Float64,
        avg_BillPayment_1 Float64,
        sum_service_amount_1 Float64,
        max_service_cnt_1 Int64,
        max_service_amount_1 Float64,
        min_service_amount_1 Float64,
        avg_OTHER_1 Float64,
        avg_DATA_BUYABLE_2 Float64,
        sum_service_cnt_2 Int64,
        avg_DATA_BOLTON_2 Float64,
        avg_Pay_Bill_2 Float64,
        avg_Recharge_Money_2 Float64,
        min_service_cnt_2 Int64,
        avg_TDD_BOLTON_2 Float64,
        avg_EREFILL_2 Float64,
        avg_BillPayment_2 Float64,
        sum_service_amount_2 Float64,
        max_service_cnt_2 Int64,
        max_service_amount_2 Float64,
        min_service_amount_2 Float64,
        avg_OTHER_2 Float64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }



  def createPostPaidFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"postpaid_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        is_creditor_1 Int32,
        unbilled_ratio_1 Float64,
        account_status_active_1 Int32,
        has_payment_1 Int32,
        credit_ratio_1 Float64,
        is_suspended_1 Int32,
        over_limit_flag_1 Int32,
        deposit_to_credit_ratio_1 Float64,
        churn_1 Int32,
        days_since_last_payment_1 Int32,
        avl_credit_limit_growth_2 Float64,
        deposit_change_2 Float64,
        total_credit_utilization_growth_2 Float64,
        credit_limit_change_2 Float64,
        credit_limit_growth_rate_2 Float64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createBankInfoFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"bankinfo_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        bank_active_days_count_1 Int64,
        bank_loyalty_ratio_first_1 Float64,
        total_bank_count_1 Int64,
        month_end_sms_ratio_1 Float64,
        avg_daily_bank_sms_first_1 Float64,
        bank_active_days_count_2 Int64,
        total_bank_count_2 Int64,
        bank_loyalty_ratio_both_2 Float64,
        month_end_sms_ratio_2 Float64,
        avg_daily_bank_sms_both_2 Float64

      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createHandsetPriceFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"handset_price_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        fake_msisdn String,
        xiaomi_usage_ratio_1 Int64,
        `handset_v[4]_1` Int64,
        max_days_single_handset_1 Int64,
        handset_stability_1 Float64,
        `handset_v[0]_1` Int64,
        `handset_v[1]_1` Int64,
        `handset_v[2]_1` Int64,
        avg_days_per_handset_1 Float64,
        apple_usage_ratio_1 Int64,
        total_unique_handsets_1 Int32,
        samsung_usage_ratio_1 Int64,
        total_unique_brands_1 Int32,
        `handset_v[3]_1` Int64,
        total_usage_days_1 Int64,
        brand_diversity_ratio_1 Float64,
        huawei_usage_ratio_1 Int64,
        xiaomi_usage_ratio_2 Int64,
        `handset_v[4]_2` Int64,
        max_days_single_handset_2 Int64,
        handset_stability_2 Float64,
        `handset_v[0]_2` Int64,
        `handset_v[1]_2` Int64,
        `handset_v[2]_2` Int64,
        avg_days_per_handset_2 Float64,
        apple_usage_ratio_2 Int64,
        total_unique_handsets_2 Int32,
        samsung_usage_ratio_2 Int64,
        total_unique_brands_2 Int32,
        `handset_v[3]_2` Int64,
        total_usage_days_2 Int64,
        brand_diversity_ratio_2 Float64,
        huawei_usage_ratio_2 Int64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }

  def createPackageFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"package_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName (
        bib_id String,
        mean_package_period_1 Float64,
        ratio_offeramount_zero_1 Float64,
        max_package_period_1 Int64,
        sum_offer_amount_1 Float64,
        count_packages_1 Int64,
        min_package_period_1 Int64,
        sum_data_MB_1 Float64,
        count_distinct_offername_1 Int64,
        count_distinct_offercode_1 Int64,
        mean_package_period_2 Float64,
        ratio_offeramount_zero_2 Float64,
        max_package_period_2 Int64,
        sum_offer_amount_2 Float64,
        count_packages_2 Int64,
        min_package_period_2 Int64,
        sum_data_MB_2 Float64,
        count_distinct_offername_2 Int64,
        count_distinct_offercode_2 Int64
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }



  def createArpuFeaturesTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    val tableName = s"arpu_features_${index}"
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS $tableName  (
        fake_msisdn String,
        sms_revenue_first_1 Float64,
        subscription_revenue_first_1 Float64,
        age_1 Float64,
        voice_revenue_first_1 Float64,
        contract_type_1 Int32,
        gprs_revenue_first_1 Float64,
        prepaid_to_postpaid_1 Int32,
        res_com_score_first_1 Float64,
        gender_1 Int32,
        site__large_city_2 Int32,
        gprs_revenue_change_2 Float64,
        `site__USO_2` Int32,
        site__Port_2 Int32,
        `site__Oil Platform_2` Int32,
        voice_revenue_change_2 Float64,
        subscription_revenue_second_2 Float64,
        voice_revenue_second_2 Float64,
        `site__Industrial Area_2` Int32,
        `site__Touristic Area_2` Int32,
        site__Island_2 Int32,
        sms_revenue_change_2 Float64,
        site__Airport_2 Int32,
        sms_revenue_second_2 Float64,
        gprs_revenue_second_2 Float64,
        site__road_village_2 Int32,
        `site__University_2` Int32
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      logger.info(s"$tableName Table Created Successfully")
    }
  }









  def createFeatureStoreTable(index: Int): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        s"""
      CREATE TABLE IF NOT EXISTS feature_store_$index (
        bib_id String,

        -- ================= CDR Features =================
        gprs_usage_sum_1 Nullable(Float64),
        sms_count_1 Nullable(Int64),
        ratio_weekend_gprs_usage_1 Nullable(Float64),
        max_time_interval_sms_1 Nullable(Int64),
        mean_time_interval_voice_1 Nullable(Float64),
        mean_time_interval_sms_1 Nullable(Float64),
        min_time_interval_gprs_1 Nullable(Int64),
        mean_time_interval_gprs_1 Nullable(Float64),
        ratio_weekend_call_duration_sum_1 Nullable(Float64),
        gprs_usage_activedays_1 Nullable(Int64),
        min_time_interval_voice_1 Nullable(Int64),
        call_duration_sum_1 Nullable(Float64),
        voice_count_1 Nullable(Int64),
        max_time_interval_voice_1 Nullable(Int64),
        max_time_interval_gprs_1 Nullable(Int64),
        sms_activedays_1 Nullable(Int64),
        min_time_interval_sms_1 Nullable(Int64),
        ratio_weekend_sms_count_1 Nullable(Float64),
        voice_activedays_1 Nullable(Int64),
        ratio_weekend_voice_count_1 Nullable(Float64),
        gprs_usage_sum_2 Nullable(Float64),
        sms_count_2 Nullable(Int64),
        ratio_weekend_gprs_usage_2 Nullable(Float64),
        max_time_interval_sms_2 Nullable(Int64),
        mean_time_interval_voice_2 Nullable(Float64),
        mean_time_interval_sms_2 Nullable(Float64),
        min_time_interval_gprs_2 Nullable(Int64),
        mean_time_interval_gprs_2 Nullable(Float64),
        ratio_weekend_call_duration_sum_2 Nullable(Float64),
        gprs_usage_activedays_2 Nullable(Int64),
        min_time_interval_voice_2 Nullable(Int64),
        call_duration_sum_2 Nullable(Float64),
        voice_count_2 Nullable(Int64),
        max_time_interval_voice_2 Nullable(Int64),
        max_time_interval_gprs_2 Nullable(Int64),
        sms_activedays_2 Nullable(Int64),
        min_time_interval_sms_2 Nullable(Int64),
        ratio_weekend_sms_count_2 Nullable(Float64),
        voice_activedays_2 Nullable(Int64),
        ratio_weekend_voice_count_2 Nullable(Float64),

        -- ================= Recharge Features =================
        count_recharge_1 Nullable(Int64),
        ratio_afternoon_recharge_1 Nullable(Float64),
        max_recharge_1 Nullable(Float64),
        min_recharge_1 Nullable(Float64),
        mean_recharge_1 Nullable(Float64),
        sum_recharge_1 Nullable(Float64),
        balance_max_1 Nullable(Float64),
        mean_balance_before_recharge_1 Nullable(Float64),
        ratio_weekend_recharge_1 Nullable(Float64),
        count_recharge_2 Nullable(Int64),
        ratio_afternoon_recharge_2 Nullable(Float64),
        max_recharge_2 Nullable(Float64),
        min_recharge_2 Nullable(Float64),
        mean_recharge_2 Nullable(Float64),
        sum_recharge_2 Nullable(Float64),
        balance_max_2 Nullable(Float64),
        mean_balance_before_recharge_2 Nullable(Float64),
        ratio_weekend_recharge_2 Nullable(Float64),

        -- ================= Credit Management Features =================
        avg_days_ontime_1 Nullable(Float64),
        avg_days_delayed_1 Nullable(Float64),
        cnt_delayed_1 Nullable(Int64),
        cnt_notdue_1 Nullable(Int64),
        cnt_ontime_1 Nullable(Int64),
        cnt_overdue_1 Nullable(Int64),
        cnt_much_delayed_notpaid_2 Nullable(Int64),
        cnt_ontime_2_2 Nullable(Int64),
        cnt_much_delayed_paid_2 Nullable(Int64),
        avg_days_ontime_2_2 Nullable(Float64),
        cnt_notdue_2_2 Nullable(Int64),
        avg_days_delayed_2_2 Nullable(Float64),
        cnt_delayed_2_2 Nullable(Int64),
        cnt_overdue_2_2 Nullable(Int64),

        -- ================= User Info Features =================
        `abstat_HARD_1` Nullable(Int32),
        `abstat_ERASED_1` Nullable(Int32),
        `abstat_OTHER_1` Nullable(Int32),
        `abstat_READY TO ACTIVATE SOFT_1` Nullable(Int32),
        `postpaid_1` Nullable(Int32),
        `max_account_balance_1` Nullable(Float64),
        `abstat_SOFT_1` Nullable(Int32),
        `bib_age_1` Nullable(Int32),
        `abstat_ACTIVE_1` Nullable(Int32),
        `abstat_READY TO ACTIVE_1` Nullable(Int32),

        -- ================= Domestic Travel Features =================
        avg_daily_travel_1 Nullable(Float64),
        unique_travel_days_1 Nullable(Int64),
        travel_1_5_1 Nullable(Int32),
        max_travel_in_day_1 Nullable(Int64),
        travel_11_plus_1 Nullable(Int32),
        travel_6_10_1 Nullable(Int32),
        total_travel_1 Nullable(Int64),
        avg_daily_travel_2 Nullable(Float64),
        unique_travel_days_2 Nullable(Int64),
        travel_1_5_2 Nullable(Int32),
        max_travel_in_day_2 Nullable(Int64),
        travel_11_plus_2 Nullable(Int32),
        travel_6_10_2 Nullable(Int32),
        total_travel_2 Nullable(Int64),

        -- ================= Loan Rec Features =================
        `mean_time_to_repay_1` Nullable(Float64),
        `mean_time_to_repay_2` Nullable(Float64),

        -- ================= Loan Assign Features =================
        `loan_amount_max_1` Nullable(Int32),
        `count_loans_1` Nullable(Int64),
        `loan_amount_min_1` Nullable(Int32),
        `max_time_interval_loan_1` Nullable(Int64),
        `loan_amount_sum_1` Nullable(Int64),
        `min_time_interval_loan_1` Nullable(Int64),
        `mean_time_interval_loan_1` Nullable(Float64),
        `loan_amount_max_2` Nullable(Int32),
        `count_loans_2` Nullable(Int64),
        `loan_amount_min_2` Nullable(Int32),
        `max_time_interval_loan_2` Nullable(Int64),
        `loan_amount_sum_2` Nullable(Int64),
        `min_time_interval_loan_2` Nullable(Int64),
        `mean_time_interval_loan_2` Nullable(Float64),

        -- ================= Package Purchase Extras Features =================
        CNT_INDIRECT_PURCHASE_1 Nullable(Int64),
        `service_v[BillPayment]_1` Nullable(Int64),
        `service_v[Pay_Bill]_1` Nullable(Int64),
        `service_v[EREFILL]_1` Nullable(Int64),
        `service_v[OTHER]_1` Nullable(Int64),
        `service_v[DATA_BOLTON]_1` Nullable(Int64),
        `service_v[Recharge_Money]_1` Nullable(Int64),
        `service_v[TDD_BOLTON]_1` Nullable(Int64),
        `service_v[DATA_BUYABLE]_1` Nullable(Int64),
        CNT_BUNDLE_PURCHASE_1 Nullable(Int64),
        CNT_DIRECT_PURCHASE_1 Nullable(Int64),
        CNT_INDIRECT_PURCHASE_2 Nullable(Int64),
        `service_v[BillPayment]_2` Nullable(Int64),
        `service_v[Pay_Bill]_2` Nullable(Int64),
        `service_v[EREFILL]_2` Nullable(Int64),
        `service_v[OTHER]_2` Nullable(Int64),
        `service_v[DATA_BOLTON]_2` Nullable(Int64),
        `service_v[Recharge_Money]_2` Nullable(Int64),
        `service_v[TDD_BOLTON]_2` Nullable(Int64),
        `service_v[DATA_BUYABLE]_2` Nullable(Int64),
        CNT_BUNDLE_PURCHASE_2 Nullable(Int64),
        CNT_DIRECT_PURCHASE_2 Nullable(Int64),

        -- ================= Package Purchase Features =================
        avg_DATA_BUYABLE_1 Nullable(Float64),
        sum_service_cnt_1 Nullable(Int64),
        avg_DATA_BOLTON_1 Nullable(Float64),
        avg_Pay_Bill_1 Nullable(Float64),
        avg_Recharge_Money_1 Nullable(Float64),
        min_service_cnt_1 Nullable(Int64),
        avg_TDD_BOLTON_1 Nullable(Float64),
        avg_EREFILL_1 Nullable(Float64),
        avg_BillPayment_1 Nullable(Float64),
        sum_service_amount_1 Nullable(Float64),
        max_service_cnt_1 Nullable(Int64),
        max_service_amount_1 Nullable(Float64),
        min_service_amount_1 Nullable(Float64),
        avg_OTHER_1 Nullable(Float64),
        avg_DATA_BUYABLE_2 Nullable(Float64),
        sum_service_cnt_2 Nullable(Int64),
        avg_DATA_BOLTON_2 Nullable(Float64),
        avg_Pay_Bill_2 Nullable(Float64),
        avg_Recharge_Money_2 Nullable(Float64),
        min_service_cnt_2 Nullable(Int64),
        avg_TDD_BOLTON_2 Nullable(Float64),
        avg_EREFILL_2 Nullable(Float64),
        avg_BillPayment_2 Nullable(Float64),
        sum_service_amount_2 Nullable(Float64),
        max_service_cnt_2 Nullable(Int64),
        max_service_amount_2 Nullable(Float64),
        min_service_amount_2 Nullable(Float64),
        avg_OTHER_2 Nullable(Float64),

        -- ================= Postpaid Features =================
        is_creditor_1 Nullable(Int32),
        unbilled_ratio_1 Nullable(Float64),
        account_status_active_1 Nullable(Int32),
        has_payment_1 Nullable(Int32),
        credit_ratio_1 Nullable(Float64),
        is_suspended_1 Nullable(Int32),
        over_limit_flag_1 Nullable(Int32),
        deposit_to_credit_ratio_1 Nullable(Float64),
        churn_1 Nullable(Int32),
        days_since_last_payment_1 Nullable(Int32),
        avl_credit_limit_growth_2 Nullable(Float64),
        deposit_change_2 Nullable(Float64),
        total_credit_utilization_growth_2 Nullable(Float64),
        credit_limit_change_2 Nullable(Float64),
        credit_limit_growth_rate_2 Nullable(Float64),

        -- ================= Bank Info Features =================
        bank_active_days_count_1 Nullable(Int64),
        bank_loyalty_ratio_first_1 Nullable(Float64),
        total_bank_count_1 Nullable(Int64),
        month_end_sms_ratio_1 Nullable(Float64),
        avg_daily_bank_sms_first_1 Nullable(Float64),
        bank_active_days_count_2 Nullable(Int64),
        total_bank_count_2 Nullable(Int64),
        bank_loyalty_ratio_both_2 Nullable(Float64),
        month_end_sms_ratio_2 Nullable(Float64),
        avg_daily_bank_sms_both_2 Nullable(Float64),

        -- ================= Handset Price Features =================
        xiaomi_usage_ratio_1 Nullable(Int64),
        `handset_v[4]_1` Nullable(Int64),
        max_days_single_handset_1 Nullable(Int64),
        handset_stability_1 Nullable(Float64),
        `handset_v[0]_1` Nullable(Int64),
        `handset_v[1]_1` Nullable(Int64),
        `handset_v[2]_1` Nullable(Int64),
        avg_days_per_handset_1 Nullable(Float64),
        apple_usage_ratio_1 Nullable(Int64),
        total_unique_handsets_1 Nullable(Int32),
        samsung_usage_ratio_1 Nullable(Int64),
        total_unique_brands_1 Nullable(Int32),
        `handset_v[3]_1` Nullable(Int64),
        total_usage_days_1 Nullable(Int64),
        brand_diversity_ratio_1 Nullable(Float64),
        huawei_usage_ratio_1 Nullable(Int64),
        xiaomi_usage_ratio_2 Nullable(Int64),
        `handset_v[4]_2` Nullable(Int64),
        max_days_single_handset_2 Nullable(Int64),
        handset_stability_2 Nullable(Float64),
        `handset_v[0]_2` Nullable(Int64),
        `handset_v[1]_2` Nullable(Int64),
        `handset_v[2]_2` Nullable(Int64),
        avg_days_per_handset_2 Nullable(Float64),
        apple_usage_ratio_2 Nullable(Int64),
        total_unique_handsets_2 Nullable(Int32),
        samsung_usage_ratio_2 Nullable(Int64),
        total_unique_brands_2 Nullable(Int32),
        `handset_v[3]_2` Nullable(Int64),
        total_usage_days_2 Nullable(Int64),
        brand_diversity_ratio_2 Nullable(Float64),
        huawei_usage_ratio_2 Nullable(Int64),

        -- ================= Package Features =================
        mean_package_period_1 Nullable(Float64),
        ratio_offeramount_zero_1 Nullable(Float64),
        max_package_period_1 Nullable(Int64),
        sum_offer_amount_1 Nullable(Float64),
        count_packages_1 Nullable(Int64),
        min_package_period_1 Nullable(Int64),
        sum_data_MB_1 Nullable(Float64),
        count_distinct_offername_1 Nullable(Int64),
        count_distinct_offercode_1 Nullable(Int64),
        mean_package_period_2 Nullable(Float64),
        ratio_offeramount_zero_2 Nullable(Float64),
        max_package_period_2 Nullable(Int64),
        sum_offer_amount_2 Nullable(Float64),
        count_packages_2 Nullable(Int64),
        min_package_period_2 Nullable(Int64),
        sum_data_MB_2 Nullable(Float64),
        count_distinct_offername_2 Nullable(Int64),
        count_distinct_offercode_2 Nullable(Int64),

        -- ================= ARPU Features =================
        sms_revenue_first_1 Nullable(Float64),
        subscription_revenue_first_1 Nullable(Float64),
        age_1 Nullable(Float64),
        voice_revenue_first_1 Nullable(Float64),
        contract_type_1 Nullable(Int32),
        gprs_revenue_first_1 Nullable(Float64),
        prepaid_to_postpaid_1 Nullable(Int32),
        res_com_score_first_1 Nullable(Float64),
        gender_1 Nullable(Int32),
        site__large_city_2 Nullable(Int32),
        gprs_revenue_change_2 Nullable(Float64),
        `site__USO_2` Nullable(Int32),
        site__Port_2 Nullable(Int32),
        `site__Oil Platform_2` Nullable(Int32),
        voice_revenue_change_2 Nullable(Float64),
        subscription_revenue_second_2 Nullable(Float64),
        voice_revenue_second_2 Nullable(Float64),
        `site__Industrial Area_2` Nullable(Int32),
        `site__Touristic Area_2` Nullable(Int32),
        site__Island_2 Nullable(Int32),
        sms_revenue_change_2 Nullable(Float64),
        site__Airport_2 Nullable(Int32),
        sms_revenue_second_2 Nullable(Float64),
        gprs_revenue_second_2 Nullable(Float64),
        site__road_village_2 Nullable(Int32),
        `site__University_2` Nullable(Int32)
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )

      logger.info("feature_store Table Created Successfully")
    } finally {
      conn.close()
    }
  }












}
