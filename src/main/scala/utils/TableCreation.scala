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
          month_index Int32,

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
        ORDER BY (bib_id, month_index)
        """
      )
    } finally {
      conn.close()
      println("CDR_feature Table Created Successfully")
    }
  }
}
