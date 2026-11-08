package io.github.matabar.kafkafim.telemetry;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * What the meters of one pipeline are labelled with and read from (DESIGN §10).
 *
 * @param kafkaCluster the source Kafka cluster ({@code kafka_cluster} label of every series)
 * @param topics source topics; their consumption counters and lag gauges exist from the start
 * @param routes route labels (the route's {@code name} or zero-based position, never an index);
 *     their {@code kafkafim_docs_total} series exist from the start
 * @param esClusters the Elasticsearch clusters the routes write to
 * @param probe the pipeline's source, for gauges
 */
public record PipelineSpec(
    String kafkaCluster,
    List<String> topics,
    List<String> routes,
    Set<String> esClusters,
    SourceProbe probe) {

  /** Copies the collections. */
  public PipelineSpec {
    Objects.requireNonNull(kafkaCluster, "kafkaCluster");
    Objects.requireNonNull(probe, "probe");
    topics = List.copyOf(topics);
    routes = List.copyOf(routes);
    esClusters = Set.copyOf(esClusters);
  }

  /** Whether {@code other} produces the same series (the probe does not count). */
  boolean sameSeries(PipelineSpec other) {
    return kafkaCluster.equals(other.kafkaCluster)
        && topics.equals(other.topics)
        && routes.equals(other.routes)
        && esClusters.equals(other.esClusters);
  }
}
