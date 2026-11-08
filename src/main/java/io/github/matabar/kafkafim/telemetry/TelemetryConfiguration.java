package io.github.matabar.kafkafim.telemetry;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.Tracer;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The telemetry beans (DESIGN §10): the §10 meters, the batch span recorder and the sampler that
 * keeps every failed batch. The supervisor feeds them; health indicators that need the supervisor
 * are declared by it.
 */
@Configuration(proxyBeanMethods = false)
public class TelemetryConfiguration {

  @Bean(destroyMethod = "close")
  KafkafimMeters kafkafimMeters(ObjectProvider<MeterRegistry> registry) {
    return new KafkafimMeters(
        registry.getIfAvailable(SimpleMeterRegistry::new), KafkafimMeters.LAG_INTERVAL);
  }

  @Bean
  BatchTracer kafkafimBatchTracer(ObjectProvider<Tracer> tracer) {
    return new BatchTracer(tracer.getIfAvailable(() -> Tracer.NOOP));
  }

  /**
   * Replaces Spring Boot's {@code parentBased(traceIdRatioBased(probability))} with the same plus
   * "failed batches always" ({@link BatchSampler}).
   */
  @Bean
  Sampler kafkafimSampler(
      @Value("${management.tracing.sampling.probability:0.1}") double probability) {
    return BatchSampler.create(probability);
  }
}
