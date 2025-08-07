package utils

import core.Core.appConfig
import java.sql.DriverManager

object TableCreation {
  def createCDRFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
        CREATE TABLE IF NOT EXISTS CDR_features (
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
      println("CDR_feature Table Created Successfully")
    }
  }
  def createRechargeFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS recharge_features (
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
      println("recharge_features Table Created Successfully")
    }
  }

  def createCreditManagementFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS credit_management_features (
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
      println("credit_management_features Table Created Successfully")
    }
  }


  def createUserInfoFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS userinfo_features (
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
      println("userinfo_features Table Created Successfully")
    }
  }

  def createDomesticTravelFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS domestic_travel_features (
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
      println("domestic_travel_features Table Created Successfully")
    }
  }

  def createLoanRecFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS loanrec_features (
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
      println("loanrec_features Table Created Successfully")
    }
  }

  def createLoanAssignFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS loanassign_features (
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
      println("LoanAssign_features Table Created Successfully")
    }
  }

  def createPackagePurchaseExtrasTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS package_purchase_extras_features (
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
      println("package_purchase_extras_features Table Created Successfully")
    }
  }



  def createPackagePurchaseFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS package_purchase_features (
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
      println("package_purchase_features Table Created Successfully")
    }
  }



  def createPostPaidFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS postpaid_features (
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
      println("postpaid_features Table Created Successfully")
    }
  }

  def createBankInfoFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS bankinfo_features (
        fake_msisdn String,
        bank_active_days_count_1 Int64,
        bank_loyalty_ratio_first_1 Float64,
        total_bank_count_1 Int64,
        month_end_sms_ratio_1 Float64,
        bank_active_days_count_2 Int64,
        total_bank_count_2 Int64,
        bank_loyalty_ratio_both_2 Float64,
        month_end_sms_ratio_2 Float64
      )
      ENGINE = MergeTree()
      ORDER BY (fake_msisdn)
      """
      )
    } finally {
      conn.close()
      println("bankinfo_features Table Created Successfully")
    }
  }

  def createHandsetPriceFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS handset_price_features (
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
      println("handset_price_features Table Created Successfully")
    }
  }

  def createPackageFeaturesTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")
    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")
    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()
      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS package_features (
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
      println("package_features Table Created Successfully")
    }
  }



  def createFeatureStoreTable(): Unit = {
    val url = appConfig.getString("clickhouse.url")
    val user = appConfig.getString("clickhouse.user")
    val password = appConfig.getString("clickhouse.password")

    Class.forName("com.clickhouse.jdbc.ClickHouseDriver")

    val conn = DriverManager.getConnection(url, user, password)
    try {
      val stmt = conn.createStatement()

      stmt.execute(
        """
      CREATE TABLE IF NOT EXISTS feature_store (
        bib_id String,
        -- CDR Features (all Nullable)
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

        -- Recharge Features (all Nullable)
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

        -- Credit Management Features (all Nullable)
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
        cnt_overdue_2_2 Nullable(Int64)
      )
      ENGINE = MergeTree()
      ORDER BY (bib_id)
      """
      )
      println("feature_store Table Created Successfully")
    } finally {
      conn.close()
    }
  }











}
