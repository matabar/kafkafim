package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** The per-pipeline reload plan (DESIGN §5.6) over bound, validated trees. */
class ConfigDiffTest {

  /**
   * Kafka cluster {@code k} with identities {@code reader} (default) and {@code dlq-writer};
   * Elasticsearch cluster {@code es} with identities {@code writer} (default) and {@code other};
   * secrets come from a configtree.
   */
  private static String tree(String kafkaExtra, String esExtra, String pipelines) {
    return ConfigTestSupport.tree(
        """
        k:
          bootstrap-servers: [k1:9092]
          security-protocol: SASL_PLAINTEXT
          identities:
            reader:
              sasl: { mechanism: SCRAM-SHA-256, username: r, password: "${kafka.k.reader.password}" }
            dlq-writer:
              sasl: { mechanism: SCRAM-SHA-256, username: d, password: "${kafka.k.dlq.password}" }
          default-identity: reader
        """
            + kafkaExtra,
        """
        es:
          uris: [http://es:9200]
          identities:
            writer: { api-key: "${elasticsearch.es.writer.api-key}" }
            other: { api-key: "${elasticsearch.es.other.api-key}" }
          default-identity: writer
        """
            + esExtra,
        pipelines);
  }

  /**
   * {@code a} uses the defaults; {@code b} writes with {@code other} and dead-letters with {@code
   * dlq-writer}; {@code c} reads another topic with the defaults.
   */
  private static final String PIPELINES =
      """
      a:
        source: { cluster: k, topics: [a] }
        routes: [ { default: true, cluster: es, index: a } ]
      b:
        source: { cluster: k, topics: [b] }
        routes: [ { default: true, cluster: es, identity: other, index: b } ]
        dlq: { identity: dlq-writer }
      c:
        source: { cluster: k, topics: [c] }
        routes: [ { default: true, cluster: es, index: c } ]
      """;

  private static final Map<String, Object> SECRETS =
      Map.of(
          "kafka.k.reader.password", "r1",
          "kafka.k.dlq.password", "d1",
          "elasticsearch.es.writer.api-key", "w1",
          "elasticsearch.es.other.api-key", "o1");

  private static KafkafimProperties bind(String yaml, Map<String, Object> secrets) {
    return ConfigTestSupport.bind(yaml, secrets);
  }

  private static KafkafimProperties base() {
    return bind(tree("", "", PIPELINES), SECRETS);
  }

  private static Map<String, Object> secrets(String key, Object value) {
    Map<String, Object> secrets = new java.util.HashMap<>(SECRETS);
    secrets.put(key, value);
    return secrets;
  }

  @Test
  void identicalTreesChangeNothing() {
    ConfigDiff diff = ConfigDiff.between(base(), base());
    assertThat(diff.isEmpty()).isTrue();
    assertThat(diff.unchanged()).containsExactly("a", "b", "c");
  }

  @Test
  void changedPipelineRestartsAlone() {
    ConfigDiff diff =
        ConfigDiff.between(
            base(), bind(tree("", "", PIPELINES.replace("index: a }", "index: a2 }")), SECRETS));
    assertThat(diff.restarted()).containsOnlyKeys("a");
    assertThat(diff.restarted().get("a")).containsExactly("pipeline definition changed");
    assertThat(diff.unchanged()).containsExactly("b", "c");
  }

  @Test
  void addedAndRemovedPipelines() {
    String d =
        """
        d:
          source: { cluster: k, topics: [d] }
          routes: [ { default: true, cluster: es, index: d } ]
        """;
    String pipelines = PIPELINES.substring(0, PIPELINES.indexOf("c:\n")) + d;
    ConfigDiff diff = ConfigDiff.between(base(), bind(tree("", "", pipelines), SECRETS));
    assertThat(diff.added()).containsExactly("d");
    assertThat(diff.removed()).containsExactly("c");
    assertThat(diff.restarted()).isEmpty();
    assertThat(diff.unchanged()).containsExactly("a", "b");
  }

  @Test
  void kafkaClusterChangeRestartsEveryPipelineReadingFromIt() {
    ConfigDiff diff =
        ConfigDiff.between(
            base(), bind(tree("  properties: { client.rack: r1 }\n", "", PIPELINES), SECRETS));
    assertThat(diff.restarted()).containsOnlyKeys("a", "b", "c");
    assertThat(diff.restarted().get("a")).containsExactly("Kafka cluster 'k' changed");
  }

  @Test
  void anElasticsearchClusterChangeRestartsEveryPipelineRoutingToIt() {
    ConfigDiff diff =
        ConfigDiff.between(base(), bind(tree("", "  compression: false\n", PIPELINES), SECRETS));
    assertThat(diff.restarted()).containsOnlyKeys("a", "b", "c");
    assertThat(diff.restarted().get("b")).containsExactly("Elasticsearch cluster 'es' changed");
  }

  @Test
  void anElasticsearchClientTimeoutChangeRestartsEveryPipelineRoutingToIt() {
    ConfigDiff diff =
        ConfigDiff.between(
            base(), bind(tree("", "  client: { response-timeout: 90s }\n", PIPELINES), SECRETS));
    assertThat(diff.restarted()).containsOnlyKeys("a", "b", "c");
    assertThat(diff.restarted().get("a")).containsExactly("Elasticsearch cluster 'es' changed");
  }

  @Test
  void anExplicitDefaultClientTimeoutIsNoChange() {
    ConfigDiff diff =
        ConfigDiff.between(
            base(),
            bind(
                tree("", "  client: { connect-timeout: 5s, response-timeout: 60s }\n", PIPELINES),
                SECRETS));
    assertThat(diff.isEmpty()).isTrue();
  }

  @Test
  void anElasticsearchIdentityChangeRestartsOnlyThePipelinesRoutingWithIt() {
    String changed =
        tree("", "", PIPELINES)
            .replace(
                "other: { api-key: \"${elasticsearch.es.other.api-key}\" }",
                "other: { username: u, password: \"${elasticsearch.es.other.api-key}\" }");
    ConfigDiff diff = ConfigDiff.between(base(), bind(changed, SECRETS));
    assertThat(diff.restarted()).containsOnlyKeys("b");
    assertThat(diff.restarted().get("b"))
        .containsExactly("Elasticsearch identity 'es/other' changed");
    assertThat(diff.unchanged()).containsExactly("a", "c");
  }

  @Test
  void kafkaIdentityChangeRestartsConsumersAndDlqProducersUsingIt() {
    ConfigDiff source =
        ConfigDiff.between(
            base(),
            bind(tree("", "", PIPELINES).replace("username: r,", "username: r2,"), SECRETS));
    // `reader` is the source identity of every pipeline and the DLQ identity of a and c.
    assertThat(source.restarted()).containsOnlyKeys("a", "b", "c");
    assertThat(source.restarted().get("b")).containsExactly("Kafka identity 'k/reader' changed");

    ConfigDiff dlq =
        ConfigDiff.between(
            base(),
            bind(tree("", "", PIPELINES).replace("username: d,", "username: d2,"), SECRETS));
    assertThat(dlq.restarted()).containsOnlyKeys("b");
    assertThat(dlq.restarted().get("b")).containsExactly("Kafka identity 'k/dlq-writer' changed");
  }

  @Test
  void rotatedSecretFileIsIdentityChange() {
    ConfigDiff es =
        ConfigDiff.between(
            base(),
            bind(tree("", "", PIPELINES), secrets("elasticsearch.es.writer.api-key", "w2")));
    assertThat(es.restarted()).containsOnlyKeys("a", "c");
    assertThat(es.restarted().get("a"))
        .containsExactly("Elasticsearch identity 'es/writer' changed");
    assertThat(es.unchanged()).containsExactly("b");

    ConfigDiff kafka =
        ConfigDiff.between(
            base(), bind(tree("", "", PIPELINES), secrets("kafka.k.dlq.password", "d2")));
    assertThat(kafka.restarted()).containsOnlyKeys("b");
  }

  @Test
  void newDefaultIdentityRestartsOnlyThePipelinesInheritingIt() {
    String changed =
        tree("", "", PIPELINES).replace("default-identity: writer", "default-identity: other");
    ConfigDiff diff = ConfigDiff.between(base(), bind(changed, SECRETS));
    // a and c now route with `other`; b named `other` explicitly.
    assertThat(diff.restarted()).containsOnlyKeys("a", "c");
    assertThat(diff.restarted().get("a")).containsExactly("pipeline definition changed");
    assertThat(diff.unchanged()).containsExactly("b");
  }

  @Test
  void anUnusedClusterChangesNoPipeline() {
    String withSpare =
        ConfigTestSupport.tree(
            "k: { bootstrap-servers: [k1:9092] }\nspare: { bootstrap-servers: [s1:9092] }\n",
            "es: { uris: [http://es:9200] }\n",
            "a:\n  source: { cluster: k, topics: [a] }\n"
                + "  routes: [ { default: true, cluster: es, index: a } ]\n");
    KafkafimProperties before = bind(withSpare, Map.of());
    KafkafimProperties after = bind(withSpare.replace("s1:9092", "s2:9092"), Map.of());
    ConfigDiff diff = ConfigDiff.between(before, after);
    assertThat(before).isNotEqualTo(after);
    assertThat(diff.isEmpty()).isTrue();
    assertThat(diff.unchanged()).isEqualTo(List.of("a"));
  }
}
