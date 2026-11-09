package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.config.KafkaCluster.SecurityProtocol;
import io.github.matabar.kafkafim.config.Pipeline.Condition;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;

class KafkafimPropertiesBindingTest {

  /** DESIGN §5.1 and §5.2 verbatim, plus the {@code search-us} cluster that §5.2 routes to. */
  static final String DESIGN_EXAMPLE =
      """
      kafkafim:
        kafka:
          clusters:
            prod-a:
              bootstrap-servers: [kafka-a-1:9093, kafka-a-2:9093]
              security-protocol: SASL_SSL
              ssl:
                truststore-location: /etc/kafkafim/secrets/kafka/prod-a/truststore.p12
                truststore-password: ${kafka.prod-a.truststore-password}
              identities:
                orders-reader:
                  sasl:
                    mechanism: SCRAM-SHA-256
                    username: ${kafka.prod-a.orders-reader.username}
                    password: ${kafka.prod-a.orders-reader.password}
                dlq-writer:
                  sasl:
                    mechanism: SCRAM-SHA-256
                    username: ${kafka.prod-a.dlq-writer.username}
                    password: ${kafka.prod-a.dlq-writer.password}
                platform-cert:
                  ssl:
                    keystore-location: /etc/kafkafim/secrets/kafka/prod-a.platform-cert/keystore.p12
                    keystore-password: ${kafka.prod-a.platform-cert.keystore-password}
              default-identity: orders-reader
              properties: {}
        elasticsearch:
          clusters:
            search-eu:
              uris: [https://es-eu-1:9200, https://es-eu-2:9200]
              ssl:
                ca-certificate: /etc/kafkafim/secrets/elasticsearch/search-eu/ca.crt
              identities:
                orders-writer:
                  username: ${elasticsearch.search-eu.orders-writer.username}
                  password: ${elasticsearch.search-eu.orders-writer.password}
                ingest-api-key:
                  api-key: ${elasticsearch.search-eu.ingest-api-key.api-key}
                platform-cert:
                  ssl:
                    keystore-location: /etc/kafkafim/secrets/elasticsearch/search-eu.platform-cert/keystore.p12
                    keystore-password: ${elasticsearch.search-eu.platform-cert.keystore-password}
              default-identity: orders-writer
              bulk:
                max-docs: 5000
                max-bytes: 5MB
                max-age: 200ms
                in-flight: 4
              compression: true
              client:
                connect-timeout: 3s
                response-timeout: 2m
            search-us:
              uris: [https://es-us-1:9200]
              identities:
                orders-writer:
                  api-key: ${elasticsearch.search-us.orders-writer.api-key}
              default-identity: orders-writer
        pipelines:
          orders:
            source:
              cluster: prod-a
              topics: [orders.v2, orders.backfill]
              group-id: kafkafim.orders
              identity: orders-reader
              offset-reset: latest
              max-poll-records: 500
              max-in-flight-bytes: 64MB
              commit-interval: 1s
              max-record-bytes: 8MB
            document:
              id-field: /order_id
              id-missing: dlq
              version-field: /version
              on-version-conflict: ignore
              timestamp-field: /@timestamp
              timestamp-format: iso-8601
              on-bad-timestamp: dlq
              op: index
            routes:
              - when:
                  - { field: /region, equals: eu }
                cluster: search-eu
                identity: orders-writer
                index: "orders-eu-%{+yyyy.MM}"
                pipeline: orders-enrich
              - when:
                  - { field: /region, in: [us, ca] }
                  - { field: /tier, exists: true }
                cluster: search-us
                index: "orders-%{region}-%{+yyyy.MM.dd}"
              - default: true
                cluster: search-eu
                identity: orders-writer
                index: orders-unrouted
            route-mode: first
            dlq:
              topic: orders.kafkafim-dlq
              identity: dlq-writer
              max-attempts: 10
              max-elapsed: 10m
            latency:
              max-age: 200ms
          orders-replay:
            source: { cluster: prod-a, topics: [orders.kafkafim-dlq], offset-reset: earliest }
            document: { id-field: /order_id }
            routes: [ { default: true, cluster: search-eu, index: "orders-eu-%{+yyyy.MM}" } ]
            dlq: { topic: orders.kafkafim-dlq-2 }
      """;

  /** What configtree would provide from the mounted Secrets. */
  static final Map<String, Object> SECRETS =
      Map.of(
          "kafka.prod-a.truststore-password", "t",
          "kafka.prod-a.orders-reader.username", "orders-reader",
          "kafka.prod-a.orders-reader.password", "p1",
          "kafka.prod-a.dlq-writer.username", "dlq-writer",
          "kafka.prod-a.dlq-writer.password", "p2",
          "kafka.prod-a.platform-cert.keystore-password", "k",
          "elasticsearch.search-eu.orders-writer.username", "orders-writer",
          "elasticsearch.search-eu.orders-writer.password", "p3",
          "elasticsearch.search-eu.ingest-api-key.api-key", "a1",
          "elasticsearch.search-eu.platform-cert.keystore-password", "k2");

  @Test
  void bindsTheDesignExampleWithPlaceholdersResolved() {
    KafkafimProperties properties = ConfigTestSupport.bind(DESIGN_EXAMPLE, withUsApiKey());

    assertThat(properties.allowInlineSecrets()).isFalse();
    KafkaCluster prodA = properties.kafka().clusters().get("prod-a");
    assertThat(prodA.bootstrapServers()).containsExactly("kafka-a-1:9093", "kafka-a-2:9093");
    assertThat(prodA.securityProtocol()).isEqualTo(SecurityProtocol.SASL_SSL);
    assertThat(prodA.ssl().truststorePassword()).isEqualTo("t");
    assertThat(prodA.identities()).containsOnlyKeys("orders-reader", "dlq-writer", "platform-cert");
    assertThat(prodA.identities().get("orders-reader").sasl().password()).isEqualTo("p1");
    assertThat(prodA.identities().get("platform-cert").ssl().keystorePassword()).isEqualTo("k");

    ElasticsearchCluster searchEu = properties.elasticsearch().clusters().get("search-eu");
    assertThat(searchEu.usesHttps()).isTrue();
    assertThat(searchEu.ssl().caCertificate()).endsWith("ca.crt");
    assertThat(searchEu.identities().get("ingest-api-key").apiKey()).isEqualTo("a1");
    assertThat(searchEu.bulk())
        .isEqualTo(
            new ElasticsearchCluster.Bulk(
                5000, DataSize.ofMegabytes(5), Duration.ofMillis(200), 4));
    assertThat(searchEu.compression()).isTrue();
    assertThat(searchEu.client())
        .isEqualTo(new ElasticsearchCluster.Client(Duration.ofSeconds(3), Duration.ofMinutes(2)));

    Pipeline orders = properties.pipelines().get("orders");
    assertThat(orders.source().topics()).containsExactly("orders.v2", "orders.backfill");
    assertThat(orders.source().maxInFlightBytes()).isEqualTo(DataSize.ofMegabytes(64));
    assertThat(orders.document().idField()).isEqualTo("/order_id");
    assertThat(orders.document().versionField()).isEqualTo("/version");
    assertThat(orders.routes()).hasSize(3);
    assertThat(orders.routes().get(0).when())
        .containsExactly(new Condition("/region", null, null, "eu", null, null, null, null));
    assertThat(orders.routes().get(1).when().get(0).oneOf()).containsExactly("us", "ca");
    assertThat(orders.routes().get(1).when().get(1).exists()).isTrue();
    assertThat(orders.routes().get(1).identity())
        .as("no identity → search-us default-identity")
        .isEqualTo("orders-writer");
    assertThat(orders.routes().get(2).isDefault()).isTrue();
    assertThat(orders.routes().get(0).pipeline()).isEqualTo("orders-enrich");
    assertThat(orders.dlq().identity()).isEqualTo("dlq-writer");
    assertThat(orders.latency().maxAge()).isEqualTo(Duration.ofMillis(200));
  }

  @Test
  void appliesTheDocumentedDefaults() {
    KafkafimProperties properties = ConfigTestSupport.bind(DESIGN_EXAMPLE, withUsApiKey());
    Pipeline replay = properties.pipelines().get("orders-replay");

    Pipeline.Source source = replay.source();
    assertThat(source.groupId()).isEqualTo("kafkafim.orders-replay");
    assertThat(source.identity()).as("cluster default-identity").isEqualTo("orders-reader");
    assertThat(source.offsetReset()).isEqualTo(Pipeline.OffsetReset.EARLIEST);
    assertThat(source.maxPollRecords()).isEqualTo(500);
    assertThat(source.maxInFlightBytes()).isEqualTo(DataSize.ofMegabytes(64));
    assertThat(source.commitInterval()).isEqualTo(Duration.ofSeconds(1));
    assertThat(source.maxRecordBytes()).isEqualTo(DataSize.ofMegabytes(8));

    Pipeline.Document document = replay.document();
    assertThat(document.idField()).isEqualTo("/order_id");
    assertThat(document.idMissing()).isEqualTo(Pipeline.IdMissing.DLQ);
    assertThat(document.versionField()).isNull();
    assertThat(document.onVersionConflict()).isEqualTo(Pipeline.OnVersionConflict.IGNORE);
    assertThat(document.timestampField()).as("ingest time").isNull();
    assertThat(document.timestampFormat()).isEqualTo("iso-8601");
    assertThat(document.onBadTimestamp()).isEqualTo(Pipeline.OnBadTimestamp.DLQ);
    assertThat(document.op()).isEqualTo(Pipeline.Op.INDEX);

    assertThat(replay.routeMode()).isEqualTo(Pipeline.RouteMode.FIRST);
    assertThat(replay.routes().get(0).identity())
        .as("route → ES cluster default-identity")
        .isEqualTo("orders-writer");
    assertThat(replay.dlq().topic()).isEqualTo("orders.kafkafim-dlq-2");
    assertThat(replay.dlq().identity())
        .as("dlq → resolved source identity")
        .isEqualTo("orders-reader");
    assertThat(replay.dlq().maxAttempts()).isEqualTo(10);
    assertThat(replay.dlq().maxElapsed()).isEqualTo(Duration.ofMinutes(10));
    assertThat(replay.latency().maxAge()).as("cluster bulk.max-age applies").isNull();
  }

  @Test
  void defaultsTheDlqTopicFromTheFirstSourceTopic() {
    KafkafimProperties properties = ConfigTestSupport.bind(ConfigTestSupport.MINIMAL);

    Pipeline pipeline = properties.pipelines().get("p");
    assertThat(pipeline.dlq().topic()).isEqualTo("t.kafkafim-dlq");
    assertThat(pipeline.source().groupId()).isEqualTo("kafkafim.p");
    assertThat(pipeline.source().identity()).isNull();
    assertThat(pipeline.dlq().identity()).isNull();

    KafkaCluster kafka = properties.kafka().clusters().get("k");
    assertThat(kafka.securityProtocol()).isEqualTo(SecurityProtocol.PLAINTEXT);
    assertThat(kafka.identities()).isEmpty();
    ElasticsearchCluster es = properties.elasticsearch().clusters().get("es");
    assertThat(es.bulk())
        .isEqualTo(
            new ElasticsearchCluster.Bulk(
                5000, DataSize.ofMegabytes(5), Duration.ofMillis(200), 4));
    assertThat(es.compression()).isTrue();
    assertThat(es.client())
        .isEqualTo(new ElasticsearchCluster.Client(Duration.ofSeconds(5), Duration.ofSeconds(60)));
  }

  @Test
  void bindsKafkaPassthroughPropertiesWithDottedKeys() {
    KafkafimProperties properties =
        ConfigTestSupport.bind(
            ConfigTestSupport.MINIMAL,
            "kafkafim.kafka.clusters.k.properties.client.dns.lookup=use_all_dns_ips",
            "kafkafim.kafka.clusters.k.properties[linger.ms]=5");

    assertThat(properties.kafka().clusters().get("k").properties())
        .containsEntry("client.dns.lookup", "use_all_dns_ips")
        .containsEntry("linger.ms", "5");
  }

  @Test
  void bindsEveryConditionType() {
    String yaml =
        ConfigTestSupport.pipeline(
            """
            routes:
              - when:
                  - { field: /a, equals: x }
                  - { field: /b, in: [x, y] }
                  - { field: /c, exists: false }
                  - { field: /d, prefix: pre }
                  - { field: /e, matches: "ab+c" }
                  - { header: source, equals: web }
                  - { topic: t }
                cluster: es
                index: idx
              - { default: true, cluster: es, index: idx }
            """);

    List<Condition> when = ConfigTestSupport.bind(yaml).pipelines().get("p").routes().get(0).when();

    assertThat(when)
        .containsExactly(
            new Condition("/a", null, null, "x", null, null, null, null),
            new Condition("/b", null, null, null, List.of("x", "y"), null, null, null),
            new Condition("/c", null, null, null, null, false, null, null),
            new Condition("/d", null, null, null, null, null, "pre", null),
            new Condition("/e", null, null, null, null, null, null, "ab+c"),
            new Condition(null, "source", null, "web", null, null, null, null),
            new Condition(null, null, "t", null, null, null, null, null));
  }

  @Test
  void rejectsUnknownProperties() {
    assertThat(
            ConfigTestSupport.runnerFailure(
                ConfigTestSupport.MINIMAL, "kafkafim.pipelines.p.source.idenity=typo"))
        .hasStackTraceContaining("kafkafim.pipelines.p.source.idenity");
  }

  private static Map<String, Object> withUsApiKey() {
    Map<String, Object> secrets = new HashMap<>(SECRETS);
    secrets.put("elasticsearch.search-us.orders-writer.api-key", "a2");
    return secrets;
  }
}
