package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.github.matabar.kafkafim.telemetry.KafkafimMeters;
import io.github.matabar.kafkafim.telemetry.PipelineSpec;
import io.github.matabar.kafkafim.telemetry.SourceProbe;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;

/**
 * Keeps the §10 meters in line with the supervisor (DESIGN §10, §5.6): at startup and after every
 * applied reload it hands {@link KafkafimMeters} the pipelines in effect, with a {@link
 * SourceProbe} per pipeline that reads whatever source the pipeline currently runs. It also answers
 * the {@code kafka} readiness contributor.
 */
final class TelemetryBinder {

  /** Bound on each broker request of a lag poll. */
  static final Duration LAG_TIMEOUT = Duration.ofSeconds(5);

  private final PipelineSupervisor supervisor;
  private final KafkafimMeters meters;

  TelemetryBinder(PipelineSupervisor supervisor, KafkafimMeters meters) {
    this.supervisor = supervisor;
    this.meters = meters;
    supervisor.onTopologyChange(this::sync);
    sync();
  }

  /** Hands the configuration in effect to the meters. */
  void sync() {
    KafkafimProperties properties = supervisor.properties();
    Map<String, PipelineSpec> specs = new LinkedHashMap<>();
    Set<String> esClusters = new LinkedHashSet<>();
    properties
        .pipelines()
        .forEach((name, pipeline) -> {
          List<String> routes = new ArrayList<>();
          Set<String> clusters = new LinkedHashSet<>();
          List<Pipeline.Route> configured = pipeline.routes();
          for (int i = 0; i < configured.size(); i++) {
            Pipeline.Route route = configured.get(i);
            routes.add(route.name() != null ? route.name() : Integer.toString(i));
            clusters.add(route.cluster());
          }
          esClusters.addAll(clusters);
          specs.put(
              name,
              new PipelineSpec(
                  pipeline.source().cluster(),
                  pipeline.source().topics(),
                  routes,
                  clusters,
                  new Probe(name)));
        });
    meters.sync(specs, esClusters, supervisor::breakerState);
  }

  /** For every started pipeline, whether its consumers are connected. */
  Map<String, Boolean> kafkaConnections() {
    Map<String, Boolean> connections = new LinkedHashMap<>();
    supervisor
        .states()
        .forEach((name, state) -> {
          if (state.isStarted()) {
            KafkaPipelineSource source = supervisor.sourceOrNull(name);
            connections.put(name, source != null && source.kafkaConnected());
          }
        });
    return connections;
  }

  /** The current source of one pipeline, read on every call. */
  private final class Probe implements SourceProbe {

    private final String pipeline;

    Probe(String pipeline) {
      this.pipeline = pipeline;
    }

    @Override
    public long inFlightBytes() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source == null ? 0 : source.inFlightBytes();
    }

    @Override
    public int assignedPartitions() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source == null ? 0 : source.assignedPartitions().size();
    }

    @Override
    public boolean connected() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      return source != null && source.kafkaConnected();
    }

    @Override
    public boolean started() {
      PipelineState state = supervisor.stateOrNull(pipeline);
      return state != null && state.isStarted();
    }

    @Override
    public Map<String, Long> lagByTopic() {
      KafkaPipelineSource source = supervisor.sourceOrNull(pipeline);
      if (source == null) {
        return Map.of();
      }
      Map<String, Long> byTopic = new LinkedHashMap<>();
      for (Map.Entry<TopicPartition, Long> lag : source.consumerLag(LAG_TIMEOUT).entrySet()) {
        byTopic.merge(lag.getKey().topic(), lag.getValue(), Long::sum);
      }
      return byTopic;
    }
  }
}
