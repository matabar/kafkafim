package io.github.matabar.kafkafim.telemetry;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * What the meters of one pipeline are labelled with and read from (DESIGN §10).
 *
 * @param kafkaCluster the source Kafka cluster ({@code kafka_cluster} label of every series)
 * @param topics source topics; their consumption counters exist from the start
 * @param routes route labels (the route's {@code name} or zero-based position, never an index);
 *     their {@code kafkafim_docs_total} series exist from the start
 * @param esClusters the Elasticsearch clusters the routes write to
 * @param esStreams the {@code (cluster, identity)} pairs the routes write with; their {@code
 *     kafkafim_auth_failures_total} series exist from the start
 * @param probe the pipeline's source, for gauges
 */
public record PipelineSpec(
    String kafkaCluster,
    List<String> topics,
    List<String> routes,
    Set<String> esClusters,
    Set<EsStream> esStreams,
    SourceProbe probe) {

  /** Copies the collections. */
  public PipelineSpec {
    Objects.requireNonNull(kafkaCluster, "kafkaCluster");
    Objects.requireNonNull(probe, "probe");
    topics = List.copyOf(topics);
    routes = List.copyOf(routes);
    esClusters = Set.copyOf(esClusters);
    esStreams = Set.copyOf(esStreams);
  }

  /** A spec without {@code (cluster, identity)} pairs: auth-failure series appear on first use. */
  public PipelineSpec(
      String kafkaCluster,
      List<String> topics,
      List<String> routes,
      Set<String> esClusters,
      SourceProbe probe) {
    this(kafkaCluster, topics, routes, esClusters, Set.of(), probe);
  }

  /** Whether {@code other} produces the same series (the probe does not count). */
  boolean sameSeries(PipelineSpec other) {
    return kafkaCluster.equals(other.kafkaCluster)
        && topics.equals(other.topics)
        && routes.equals(other.routes)
        && esClusters.equals(other.esClusters)
        && esStreams.equals(other.esStreams);
  }

  /**
   * One Elasticsearch cluster and the identity a route writes to it with.
   *
   * @param esCluster the Elasticsearch cluster name
   * @param identity the resolved identity, or {@code null} for an unauthenticated cluster ({@code
   *     identity="none"})
   */
  public record EsStream(String esCluster, @Nullable String identity) {

    /** Requires the cluster. */
    public EsStream {
      Objects.requireNonNull(esCluster, "esCluster");
    }

    /** The {@code identity} label value. */
    String identityLabel() {
      return identity == null ? TelemetryNames.NONE : identity;
    }
  }
}
