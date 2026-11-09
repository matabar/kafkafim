package io.github.matabar.kafkafim.config;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * What a reload changes, pipeline by pipeline (DESIGN §5.6). Both trees must be valid.
 *
 * <ul>
 *   <li><b>added</b>: in the new tree only; <b>removed</b>: in the old tree only.
 *   <li><b>restarted</b>: in both, and any of: the pipeline's own definition differs (with its
 *       resolved group id and identities, so a changed {@code default-identity} counts for the
 *       pipelines that inherit it); a Kafka or Elasticsearch <em>cluster</em> it uses differs in
 *       anything but its identities and {@code default-identity} (connection, trust, protocol,
 *       passthrough properties, bulk settings, compression, client timeouts); an <em>identity</em>
 *       it references differs. A pipeline uses its source cluster (consumer and DLQ producer) and
 *       the cluster of every route; it references its source identity, its DLQ identity and every
 *       route's identity. Secret values are part of the bound identities, so a rotated secret file
 *       is an identity change.
 *   <li><b>unchanged</b>: everything else; such pipelines are not touched.
 * </ul>
 *
 * <p>Reasons name clusters and identities, never values, so they can be logged and returned.
 *
 * @param added new pipelines, in new-tree order
 * @param removed pipelines that are gone, in old-tree order
 * @param restarted changed pipelines with why, in new-tree order
 * @param unchanged untouched pipelines, in new-tree order
 */
public record ConfigDiff(
    List<String> added,
    List<String> removed,
    Map<String, List<String>> restarted,
    List<String> unchanged) {

  /** Copies the collections, keeping their order. */
  public ConfigDiff {
    added = List.copyOf(added);
    removed = List.copyOf(removed);
    restarted = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(restarted));
    unchanged = List.copyOf(unchanged);
  }

  /** Whether no pipeline has to start, stop or restart. */
  public boolean isEmpty() {
    return added.isEmpty() && removed.isEmpty() && restarted.isEmpty();
  }

  /**
   * Compares two valid trees.
   *
   * @param previous the running configuration
   * @param next the configuration to apply
   * @return the per-pipeline plan
   */
  public static ConfigDiff between(KafkafimProperties previous, KafkafimProperties next) {
    List<String> added = new ArrayList<>();
    Map<String, List<String>> restarted = new LinkedHashMap<>();
    List<String> unchanged = new ArrayList<>();
    for (Map.Entry<String, Pipeline> entry : next.pipelines().entrySet()) {
      String name = entry.getKey();
      Pipeline before = previous.pipelines().get(name);
      if (before == null) {
        added.add(name);
        continue;
      }
      List<String> reasons = reasons(before, entry.getValue(), previous, next);
      if (reasons.isEmpty()) {
        unchanged.add(name);
      } else {
        restarted.put(name, reasons);
      }
    }
    List<String> removed = new ArrayList<>();
    for (String name : previous.pipelines().keySet()) {
      if (!next.pipelines().containsKey(name)) {
        removed.add(name);
      }
    }
    return new ConfigDiff(added, removed, restarted, unchanged);
  }

  private static List<String> reasons(
      Pipeline before, Pipeline after, KafkafimProperties previous, KafkafimProperties next) {
    List<String> reasons = new ArrayList<>();
    if (!before.equals(after)) {
      reasons.add("pipeline definition changed");
    }
    // `after` is checked against both trees: if the definition is unchanged, before and after
    // reference the same clusters and identities; if it changed, the pipeline restarts anyway.
    String kafkaCluster = after.source().cluster();
    KafkaCluster kafkaBefore = previous.kafka().clusters().get(kafkaCluster);
    KafkaCluster kafkaAfter = next.kafka().clusters().get(kafkaCluster);
    if (!Objects.equals(withoutIdentities(kafkaBefore), withoutIdentities(kafkaAfter))) {
      reasons.add("Kafka cluster '" + kafkaCluster + "' changed");
    }
    Set<String> kafkaIdentities = new LinkedHashSet<>();
    addIfSet(kafkaIdentities, after.source().identity());
    addIfSet(kafkaIdentities, after.dlq().identity());
    for (String identity : kafkaIdentities) {
      if (!Objects.equals(
          kafkaBefore == null ? null : kafkaBefore.identities().get(identity),
          kafkaAfter == null ? null : kafkaAfter.identities().get(identity))) {
        reasons.add("Kafka identity '" + kafkaCluster + "/" + identity + "' changed");
      }
    }
    Set<String> esClusters = new LinkedHashSet<>();
    Set<List<String>> esIdentities = new LinkedHashSet<>();
    for (Pipeline.Route route : after.routes()) {
      esClusters.add(route.cluster());
      if (route.identity() != null) {
        esIdentities.add(List.of(route.cluster(), route.identity()));
      }
    }
    for (String cluster : esClusters) {
      if (!Objects.equals(
          withoutIdentities(previous.elasticsearch().clusters().get(cluster)),
          withoutIdentities(next.elasticsearch().clusters().get(cluster)))) {
        reasons.add("Elasticsearch cluster '" + cluster + "' changed");
      }
    }
    for (List<String> reference : esIdentities) {
      String cluster = reference.get(0);
      String identity = reference.get(1);
      if (!Objects.equals(
          esIdentity(previous, cluster, identity), esIdentity(next, cluster, identity))) {
        reasons.add("Elasticsearch identity '" + cluster + "/" + identity + "' changed");
      }
    }
    return reasons;
  }

  private static void addIfSet(Set<String> names, @Nullable String name) {
    if (name != null) {
      names.add(name);
    }
  }

  private static ElasticsearchCluster.@Nullable ElasticsearchIdentity esIdentity(
      KafkafimProperties properties, String cluster, String identity) {
    ElasticsearchCluster definition = properties.elasticsearch().clusters().get(cluster);
    return definition == null ? null : definition.identities().get(identity);
  }

  /** The cluster-wide part of a Kafka cluster: everything but identities and their default. */
  private static @Nullable KafkaCluster withoutIdentities(@Nullable KafkaCluster cluster) {
    return cluster == null
        ? null
        : new KafkaCluster(
            cluster.bootstrapServers(),
            cluster.securityProtocol(),
            cluster.ssl(),
            null,
            null,
            cluster.properties());
  }

  /** The cluster-wide part of an Elasticsearch cluster: everything but identities and default. */
  private static @Nullable ElasticsearchCluster withoutIdentities(
      @Nullable ElasticsearchCluster cluster) {
    return cluster == null
        ? null
        : new ElasticsearchCluster(
            cluster.uris(),
            cluster.ssl(),
            null,
            null,
            cluster.bulk(),
            cluster.compression(),
            cluster.client());
  }
}
