package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import io.github.matabar.kafkafim.telemetry.BatchTracer;
import io.github.matabar.kafkafim.telemetry.KafkaHealthIndicator;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.SupervisorHealthIndicator;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;

/**
 * Wires the {@link PipelineSupervisor}, the pool of building blocks it hands to pipelines ({@link
 * PipelineResources}) and hot reload ({@link ConfigReloader}, {@link ReloadEndpoint}). The sinks'
 * and DLQ producers' only listener is a {@link ListenerRelay}; telemetry adds {@link SinkListener},
 * {@link DlqListener} or {@link SupervisorListener} beans, which the relay and the supervisor
 * forward to. The §10 meters and spans ({@code telemetry}) are fed by a {@link TelemetryListener}
 * (sink and DLQ events), the pipelines' data path (consumption, documents) and a {@link
 * TelemetryBinder} (series per configured pipeline, gauges, the {@code kafka} readiness and {@code
 * supervisor} liveness contributors).
 *
 * <p>The {@link KafkafimProperties} bean is the configuration bound at startup (generation 1);
 * after a reload, {@link PipelineSupervisor#properties()} is the one in effect.
 *
 * <p>Shutdown order (DESIGN §7): {@link ConfigReloader} stops the watcher (phase {@code MAX_VALUE -
 * 50}); {@link PipelineSupervisor} (phase {@code MAX_VALUE - 100}: sources drain, then sinks
 * close); {@link PipelineResources} closes the {@link DlqProducers} (phase {@code MAX_VALUE -
 * 1000}); the web server ({@code MAX_VALUE - 1024}); the {@link ElasticsearchSinks} clients close
 * when the context is destroyed, after every lifecycle phase.
 */
@Configuration(proxyBeanMethods = false)
public class SupervisorConfiguration {

  @Bean
  TelemetryListener kafkafimTelemetryListener(KafkafimMeters meters, BatchTracer tracer) {
    return new TelemetryListener(meters, tracer);
  }

  @Bean
  ListenerRelay kafkafimListenerRelay(
      ObjectProvider<SinkListener> sinkListeners, ObjectProvider<DlqListener> dlqListeners) {
    return new ListenerRelay(
        sinkListeners.orderedStream().toList(), dlqListeners.orderedStream().toList());
  }

  @Bean(destroyMethod = "close")
  PipelineResources kafkafimPipelineResources(ListenerRelay relay) {
    return new PipelineResources(
        relay,
        properties -> new ElasticsearchAuthCheck(properties.elasticsearch().clusters()),
        PipelineResources.Settings.DEFAULTS);
  }

  @Bean
  PipelineSupervisor kafkafimPipelineSupervisor(
      KafkafimProperties properties,
      PipelineResources resources,
      ListenerRelay relay,
      ObjectProvider<SupervisorListener> listeners,
      KafkafimMeters meters) {
    PipelineSupervisor supervisor =
        new PipelineSupervisor(
            properties, resources, listeners.orderedStream().toList(), meters::pipeline);
    relay.attach(supervisor);
    return supervisor;
  }

  @Bean
  PipelineStateMetrics kafkafimPipelineStateMetrics(PipelineSupervisor supervisor) {
    return new PipelineStateMetrics(supervisor);
  }

  @Bean
  TelemetryBinder kafkafimTelemetryBinder(PipelineSupervisor supervisor, KafkafimMeters meters) {
    return new TelemetryBinder(supervisor, meters);
  }

  @Bean
  PipelinesHealthIndicator pipelinesHealthIndicator(PipelineSupervisor supervisor) {
    return new PipelinesHealthIndicator(supervisor);
  }

  /** {@code kafka}, in the readiness group: Kafka clients connected (DESIGN §10). */
  @Bean
  KafkaHealthIndicator kafkaHealthIndicator(TelemetryBinder binder) {
    return new KafkaHealthIndicator(binder::kafkaConnections);
  }

  /** {@code supervisor}, in the liveness group: the supervisor runs (DESIGN §10). */
  @Bean
  SupervisorHealthIndicator supervisorHealthIndicator(PipelineSupervisor supervisor) {
    return new SupervisorHealthIndicator(supervisor::isRunning, supervisor::isShuttingDown);
  }

  @Bean
  ConfigReloader kafkafimConfigReloader(
      ConfigurableEnvironment environment,
      PipelineSupervisor supervisor,
      ObjectProvider<MeterRegistry> registry) {
    return new ConfigReloader(
        environment, supervisor, registry.getIfAvailable(SimpleMeterRegistry::new));
  }

  @Bean
  ReloadEndpoint kafkafimReloadEndpoint(ConfigReloader reloader) {
    return new ReloadEndpoint(reloader);
  }
}
