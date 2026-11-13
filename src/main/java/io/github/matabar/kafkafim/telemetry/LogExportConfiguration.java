package io.github.matabar.kafkafim.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Connects the {@code OTLP} appender of {@code logback-spring.xml} to the OpenTelemetry SDK that
 * Spring Boot auto-configures, so log records leave over OTLP with the metrics' and traces'
 * resource attributes and the current span's trace id (DESIGN §10 Logs, ADR-0041).
 *
 * <p>Spring Boot provides the logger provider, the batching processor and the OTLP exporter; this
 * bridge from Logback is the one piece it leaves to the application. Until it runs, the appender
 * keeps the events logged so far and replays them. With OpenTelemetry or the log export switched
 * off, the installed SDK has no log exporter and the appender's records go nowhere.
 */
@Configuration(proxyBeanMethods = false)
public class LogExportConfiguration {

  @Bean
  InitializingBean kafkafimLogExportBridge(OpenTelemetry openTelemetry) {
    return () -> OpenTelemetryAppender.install(openTelemetry);
  }
}
