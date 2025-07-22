package metrics

import io.prometheus.client.{CollectorRegistry, Gauge}
import io.prometheus.client.exporter.PushGateway

object MetricsPusher {
  private val registry = new CollectorRegistry()
  private val durationGauge = Gauge.build()
    .name("feature_engineering_duration_seconds")
    .help("Duration of feature engineering job in seconds")
    .register(registry)

  def push(durationSeconds: Double, succeeded: Boolean): Unit = {
    durationGauge.set(durationSeconds)

    // Add success status as a gauge (optional)
    val successGauge = Gauge.build()
      .name("feature_engineering_success")
      .help("Feature engineering job success status (1=success, 0=failure)")
      .register(registry)

    successGauge.set(if (succeeded) 1 else 0)

    val pg = new PushGateway("localhost:9091") // Default PushGateway
    pg.pushAdd(registry, "feature_engineering_job")
  }
}
