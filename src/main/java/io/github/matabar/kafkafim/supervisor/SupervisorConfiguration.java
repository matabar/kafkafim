package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.sink.es.SinkListener;
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
 * forward to.
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
      ObjectProvider<SupervisorListener> listeners) {
    PipelineSupervisor supervisor =
        new PipelineSupervisor(properties, resources, listeners.orderedStream().toList());
    relay.attach(supervisor);
    return supervisor;
  }

  @Bean
  PipelineStateMetrics kafkafimPipelineStateMetrics(PipelineSupervisor supervisor) {
    return new PipelineStateMetrics(supervisor);
  }

  @Bean
  PipelinesHealthIndicator pipelinesHealthIndicator(PipelineSupervisor supervisor) {
    return new PipelinesHealthIndicator(supervisor);
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
