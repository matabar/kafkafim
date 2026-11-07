package io.github.matabar.kafkafim.config;

import jakarta.validation.Valid;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * The whole {@code kafkafim.*} configuration tree (DESIGN §5), immutable once bound.
 *
 * <p>Defaults are applied while binding: the group id ({@code kafkafim.<pipeline name>}) and the
 * identity resolution of §5.2.1 (source → cluster {@code default-identity}; DLQ → resolved source
 * identity; route → cluster {@code default-identity}). A default identity is only applied when the
 * referenced cluster exists and declares it; otherwise the reference stays unset and {@link
 * ConfigValidator} reports the problem once, at its origin.
 *
 * @param allowInlineSecrets accept literal secret values (local development only, §5.5)
 * @param kafka Kafka cluster definitions
 * @param elasticsearch Elasticsearch cluster definitions
 * @param pipelines pipelines by name
 */
@ConfigurationProperties(prefix = KafkafimProperties.PREFIX, ignoreUnknownFields = false)
@Validated
public record KafkafimProperties(
    boolean allowInlineSecrets,
    @Valid Kafka kafka,
    @Valid Elasticsearch elasticsearch,
    Map<String, @Valid Pipeline> pipelines) {

  /** Property prefix of the tree. */
  public static final String PREFIX = "kafkafim";

  /** Prefix of default consumer group ids. */
  public static final String GROUP_ID_PREFIX = "kafkafim.";

  /** Applies defaults that depend on pipeline names and cluster definitions. */
  public KafkafimProperties {
    kafka = Defaults.orDefault(kafka, new Kafka(null));
    elasticsearch = Defaults.orDefault(elasticsearch, new Elasticsearch(null));
    Map<String, Pipeline> resolved = new LinkedHashMap<>();
    for (Map.Entry<String, Pipeline> entry : Defaults.map(pipelines).entrySet()) {
      resolved.put(entry.getKey(), resolve(entry.getKey(), entry.getValue(), kafka, elasticsearch));
    }
    pipelines = Defaults.map(resolved);
  }

  private static Pipeline resolve(
      String name, Pipeline pipeline, Kafka kafka, Elasticsearch elasticsearch) {
    if (pipeline == null) {
      return null;
    }
    Pipeline.Source source = pipeline.source();
    String groupId = Defaults.orDefault(source.groupId(), GROUP_ID_PREFIX + name);
    KafkaCluster kafkaCluster =
        source.cluster() == null ? null : kafka.clusters().get(source.cluster());
    String sourceIdentity = source.identity();
    if (sourceIdentity == null && kafkaCluster != null) {
      sourceIdentity = usableDefault(kafkaCluster.defaultIdentity(), kafkaCluster.identities());
    }
    Pipeline.Dlq dlq = pipeline.dlq();
    if (dlq.identity() == null) {
      dlq = dlq.withIdentity(sourceIdentity);
    }
    return new Pipeline(
        source.withGroupIdAndIdentity(groupId, sourceIdentity),
        pipeline.document(),
        pipeline.routes().stream().map(route -> resolve(route, elasticsearch)).toList(),
        pipeline.routeMode(),
        dlq,
        pipeline.latency());
  }

  private static Pipeline.Route resolve(Pipeline.Route route, Elasticsearch elasticsearch) {
    if (route == null || route.identity() != null || route.cluster() == null) {
      return route;
    }
    ElasticsearchCluster cluster = elasticsearch.clusters().get(route.cluster());
    if (cluster == null) {
      return route;
    }
    String identity = usableDefault(cluster.defaultIdentity(), cluster.identities());
    return identity == null ? route : route.withIdentity(identity);
  }

  private static String usableDefault(String defaultIdentity, Map<String, ?> identities) {
    return defaultIdentity != null && identities.containsKey(defaultIdentity)
        ? defaultIdentity
        : null;
  }

  /**
   * {@code kafkafim.kafka}.
   *
   * @param clusters Kafka clusters by name
   */
  public record Kafka(Map<String, @Valid KafkaCluster> clusters) {

    /** Applies the documented defaults. */
    public Kafka {
      clusters = Defaults.map(clusters);
    }
  }

  /**
   * {@code kafkafim.elasticsearch}.
   *
   * @param clusters Elasticsearch clusters by name
   */
  public record Elasticsearch(Map<String, @Valid ElasticsearchCluster> clusters) {

    /** Applies the documented defaults. */
    public Elasticsearch {
      clusters = Defaults.map(clusters);
    }
  }
}
