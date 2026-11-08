package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.sink.es.BreakerSettings;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import io.github.matabar.kafkafim.source.kafka.ConsumerFactories;
import io.github.matabar.kafkafim.source.kafka.SourceOptions;
import java.time.Clock;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the building blocks of one bound configuration into the {@link PipelineSupervisor}. The
 * sinks' and DLQ producers' only listener is a {@link ListenerRelay}; telemetry adds {@link
 * SinkListener}, {@link DlqListener} or {@link SupervisorListener} beans, which the relay and the
 * supervisor forward to.
 *
 * <p>Shutdown order (DESIGN §7): {@link PipelineSupervisor} (phase {@code MAX_VALUE - 100}: sources
 * drain, then sinks close), {@link DlqProducers} (phase {@code MAX_VALUE - 1000}), the web server
 * ({@code MAX_VALUE - 1024}); {@link ElasticsearchSinks} closes its clients when the context is
 * destroyed, after every lifecycle phase.
 */
@Configuration(proxyBeanMethods = false)
public class SupervisorConfiguration {

  @Bean
  ListenerRelay kafkafimListenerRelay(
      ObjectProvider<SinkListener> sinkListeners, ObjectProvider<DlqListener> dlqListeners) {
    return new ListenerRelay(
        sinkListeners.orderedStream().toList(), dlqListeners.orderedStream().toList());
  }

  @Bean
  DlqProducers kafkafimDlqProducers(KafkafimProperties properties, ListenerRelay relay) {
    return new DlqProducers(properties, relay.dlqListener());
  }

  @Bean(destroyMethod = "close")
  ElasticsearchSinks kafkafimElasticsearchSinks(
      KafkafimProperties properties, ListenerRelay relay) {
    return new ElasticsearchSinks(
        properties.elasticsearch().clusters(),
        ClientSettings.DEFAULT,
        BreakerSettings.DEFAULT,
        relay.sinkListener());
  }

  @Bean
  ConsumerFactories kafkafimConsumerFactories(KafkafimProperties properties) {
    return new ConsumerFactories(properties);
  }

  @Bean
  PipelineSupervisor kafkafimPipelineSupervisor(
      KafkafimProperties properties,
      ConsumerFactories consumerFactories,
      DlqProducers dlqProducers,
      ElasticsearchSinks sinks,
      ListenerRelay relay,
      ObjectProvider<SupervisorListener> listeners) {
    PipelineSupervisor supervisor =
        new PipelineSupervisor(
            new PipelineRuntime.Context(
                properties,
                consumerFactories,
                dlqProducers,
                sinks,
                new ElasticsearchAuthCheck(properties.elasticsearch().clusters()),
                SourceOptions.defaults(),
                PipelineSupervisor.SINK_CLOSE_TIMEOUT,
                Clock.systemUTC()),
            listeners.orderedStream().toList());
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
}
