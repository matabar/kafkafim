package io.github.matabar.kafkafim.config;

import static io.github.matabar.kafkafim.config.ConfigTestSupport.ES_CLUSTERS;
import static io.github.matabar.kafkafim.config.ConfigTestSupport.KAFKA_CLUSTERS;
import static io.github.matabar.kafkafim.config.ConfigTestSupport.MINIMAL;
import static io.github.matabar.kafkafim.config.ConfigTestSupport.pipeline;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** One valid and one invalid case per validation rule, through the real binding path. */
class ConfigValidationTest {

  private static final String P = "kafkafim.pipelines.p";
  private static final String K = "kafkafim.kafka.clusters.k";
  private static final String ES = "kafkafim.elasticsearch.clusters.es";

  static void assertValid(String yaml, String... overrides) {
    ConfigTestSupport.bind(yaml, overrides);
  }

  static void assertInvalid(String yaml, String expected, String... overrides) {
    assertThat(ConfigTestSupport.errors(yaml, overrides)).anyMatch(e -> e.contains(expected));
  }

  @Test
  void reportsEveryViolationInOneFailure() {
    List<String> errors =
        ConfigTestSupport.errors(
            MINIMAL,
            P + ".source.cluster=nope",
            P + ".routes[0].cluster=nope",
            P + ".routes[0].default=true",
            P + ".routes[0].index=bad%{",
            P + ".document.id-field=id",
            K + ".bootstrap-servers=k1");

    assertThat(errors)
        .contains(
            P + ".source.cluster: unknown Kafka cluster 'nope'",
            P + ".routes[0].cluster: unknown Elasticsearch cluster 'nope'",
            P + ".routes[0].index: unterminated '%{' at position 3",
            P + ".document.id-field: 'id' is not a JSON pointer (must start with '/')",
            K + ".bootstrap-servers[0]: 'k1' is not host:port");
  }

  @Test
  void reportsBeanValidationAndCrossReferenceErrorsTogether() {
    List<String> errors =
        ConfigTestSupport.errors(MINIMAL, P + ".source.cluster=", P + ".document.id-field=x");

    assertThat(errors)
        .anyMatch(e -> e.startsWith("pipelines[p].source.cluster: "))
        .contains(P + ".document.id-field: 'x' is not a JSON pointer (must start with '/')");
  }

  @Nested
  class Names {

    @Test
    void acceptsKebabCaseNames() {
      assertValid(
          pipeline(
              "orders-eu-1:\n  bootstrap-servers: [k1:9092]\n",
              ES_CLUSTERS,
              "source: { cluster: orders-eu-1, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE));
    }

    @Test
    void rejectsOtherNames() {
      assertInvalid(
          pipeline(
              "Orders_EU:\n  bootstrap-servers: [k1:9092]\n",
              ES_CLUSTERS,
              "source: { cluster: Orders_EU, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE),
          "Kafka cluster name 'Orders_EU' must be lower-case letters, digits and '-'");
    }
  }

  @Nested
  class KafkaClusters {

    @Test
    void acceptsHostPortBootstrapServers() {
      assertValid(MINIMAL, K + ".bootstrap-servers=k1:9092,[::1]:9093,PLAINTEXT://k3:9094");
    }

    @Test
    void rejectsBootstrapServersWithoutPort() {
      assertInvalid(MINIMAL, "'k2' is not host:port", K + ".bootstrap-servers=k1:9092,k2");
      assertInvalid(MINIMAL, "'k1:70000' is not host:port", K + ".bootstrap-servers=k1:70000");
    }

    @Test
    void rejectsMissingBootstrapServers() {
      assertInvalid(
          pipeline(
              "k:\n  security-protocol: PLAINTEXT\n", ES_CLUSTERS, ConfigTestSupport.DEFAULT_ROUTE),
          "kafka.clusters[k].bootstrapServers: must not be empty");
    }

    @Test
    void acceptsKnownDefaultIdentity() {
      assertValid(
          MINIMAL,
          K + ".security-protocol=SASL_SSL",
          sasl("reader"),
          K + ".default-identity=reader");
    }

    @Test
    void rejectsAnUnknownDefaultIdentity() {
      assertInvalid(
          MINIMAL,
          K + ".default-identity: 'nope' is not one of this cluster's identities [reader]",
          sasl("reader"),
          K + ".default-identity=nope");
    }

    @Test
    void rejectsAnIdentityWithoutCredentials() {
      KafkaCluster cluster =
          new KafkaCluster(
              List.of("k1:9092"),
              null,
              null,
              Map.of("empty", new KafkaCluster.KafkaIdentity(null, null)),
              null,
              null);
      KafkafimProperties properties =
          new KafkafimProperties(
              false, new KafkafimProperties.Kafka(Map.of("k", cluster)), null, null);

      assertThat(ConfigValidator.validate(properties).errors())
          .containsExactly(
              K
                  + ".identities.empty: a Kafka identity needs a sasl block, an ssl keystore"
                  + " block, or both");
    }

    @Test
    void acceptsSaslOnSaslProtocol() {
      assertValid(
          MINIMAL,
          K + ".security-protocol=SASL_PLAINTEXT",
          sasl("reader"),
          P + ".source.identity=reader");
    }

    @Test
    void rejectsSaslOnNonSaslProtocol() {
      assertInvalid(
          MINIMAL,
          K + ".identities.reader.sasl: sasl is set but security-protocol PLAINTEXT is not SASL_*",
          sasl("reader"));
    }

    @Test
    void rejectsScramWithoutPassword() {
      assertInvalid(
          MINIMAL,
          K + ".identities.reader.sasl.password: SCRAM-SHA-256 needs a password",
          K + ".security-protocol=SASL_SSL",
          K + ".identities.reader.sasl.mechanism=SCRAM-SHA-256",
          K + ".identities.reader.sasl.username=u");
    }

    @Test
    void acceptsGssapiWithoutPassword() {
      assertValid(
          MINIMAL,
          K + ".security-protocol=SASL_SSL",
          K + ".identities.reader.sasl.mechanism=GSSAPI",
          K + ".default-identity=reader");
    }

    @Test
    void acceptsKeystoreAndTrustOverTls() {
      assertValid(
          MINIMAL,
          K + ".security-protocol=SSL",
          K + ".ssl.truststore-location=/t.p12",
          K + ".identities.cert.ssl.keystore-location=/k.p12");
    }

    @Test
    void rejectsKeystoreAndTrustWithoutTls() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL,
              K + ".security-protocol=SASL_PLAINTEXT",
              K + ".ssl.truststore-location=/t.p12",
              K + ".identities.cert.ssl.keystore-location=/k.p12",
              sasl("reader"),
              K + ".default-identity=reader");

      assertThat(errors)
          .contains(
              K + ".ssl: trust is set but security-protocol SASL_PLAINTEXT does not use TLS",
              K
                  + ".identities.cert.ssl: a client keystore is set but security-protocol"
                  + " SASL_PLAINTEXT does not use TLS");
    }

    @Test
    void rejectsTrustWithoutLocation() {
      assertInvalid(
          MINIMAL,
          "kafka.clusters[k].ssl.truststoreLocation: must not be blank",
          K + ".security-protocol=SSL",
          K + ".ssl.truststore-type=PKCS12");
    }
  }

  @Nested
  class ElasticsearchClusters {

    @Test
    void acceptsHttpsUris() {
      assertValid(
          MINIMAL,
          ES + ".uris=https://a:9200,https://b:9200",
          basic("w"),
          ES + ".default-identity=w");
    }

    @Test
    void rejectsOtherSchemes() {
      assertInvalid(
          MINIMAL,
          ES + ".uris[0]: 'ftp://a' must start with http:// or https://",
          ES + ".uris=ftp://a");
    }

    @Test
    void rejectsMixedSchemes() {
      assertInvalid(
          MINIMAL, ES + ".uris: mixes http and https", ES + ".uris=http://a:9200,https://b:9200");
    }

    @Test
    void acceptsEachCredentialKindAndKeystoreCombinations() {
      assertValid(
          MINIMAL,
          ES + ".uris=https://a:9200",
          ES + ".default-identity=basic",
          basic("basic"),
          ES + ".identities.key.api-key=key",
          ES + ".identities.cert.ssl.keystore-location=/k.p12",
          basic("cert-basic"),
          ES + ".identities.cert-basic.ssl.keystore-location=/k.p12",
          ES + ".identities.cert-key.api-key=key",
          ES + ".identities.cert-key.ssl.keystore-location=/k.p12");
    }

    @Test
    void rejectsBasicAndApiKeyTogether() {
      assertInvalid(
          MINIMAL,
          ES + ".identities.w: set either username+password or api-key, not both",
          basic("w"),
          ES + ".identities.w.api-key=key");
    }

    @Test
    void rejectsIncompleteBasicAuth() {
      assertInvalid(
          MINIMAL,
          ES + ".identities.w.password: basic auth needs a password",
          ES + ".identities.w.username=u");
    }

    @Test
    void rejectsAnIdentityWithoutCredentials() {
      ElasticsearchCluster cluster =
          new ElasticsearchCluster(
              List.of("http://es:9200"),
              null,
              Map.of("w", new ElasticsearchCluster.ElasticsearchIdentity(null, null, null, null)),
              null,
              null,
              null,
              null);
      KafkafimProperties properties =
          new KafkafimProperties(
              false, null, new KafkafimProperties.Elasticsearch(Map.of("es", cluster)), null);

      assertThat(ConfigValidator.validate(properties).errors())
          .containsExactly(
              ES
                  + ".identities.w: an Elasticsearch identity needs username+password, api-key"
                  + " or an ssl keystore");
    }

    @Test
    void rejectsKeystoreAndTrustOverHttp() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL,
              ES + ".ssl.ca-certificate=/ca.crt",
              ES + ".identities.c.ssl.keystore-location=/k");

      assertThat(errors)
          .contains(
              ES + ".ssl: trust is set but the cluster URIs are not https",
              ES + ".identities.c.ssl: a client keystore needs https cluster URIs");
    }

    @Test
    void rejectsNonPositiveBulkLimits() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL, ES + ".bulk.max-docs=0", ES + ".bulk.max-bytes=0B", ES + ".bulk.max-age=0s");

      assertThat(errors)
          .contains(
              ES + ".bulk.max-bytes: must be positive", ES + ".bulk.max-age: must be positive")
          .anyMatch(e -> e.startsWith("elasticsearch.clusters[es].bulk.maxDocs: "));
    }

    @Test
    void rejectsNonPositiveClientTimeouts() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL, ES + ".client.connect-timeout=0s", ES + ".client.response-timeout=-1s");

      assertThat(errors)
          .contains(
              ES + ".client.connect-timeout: must be positive",
              ES + ".client.response-timeout: must be positive");
    }
  }

  @Nested
  class IdentityResolution {

    private static final String SASL_SSL = K + ".security-protocol=SASL_SSL";

    @Test
    void sourceFallsBackToTheClusterDefault() {
      KafkafimProperties properties =
          ConfigTestSupport.bind(MINIMAL, SASL_SSL, sasl("reader"), K + ".default-identity=reader");

      assertThat(properties.pipelines().get("p").source().identity()).isEqualTo("reader");
      assertThat(properties.pipelines().get("p").dlq().identity()).isEqualTo("reader");
    }

    @Test
    void failsWhenAuthIsRequiredAndNoIdentityResolves() {
      List<String> errors = ConfigTestSupport.errors(MINIMAL, SASL_SSL, sasl("reader"));

      assertThat(errors)
          .containsExactly(
              P
                  + ".source.identity: Kafka cluster 'k' uses SASL_SSL and no identity resolves;"
                  + " set this identity or the cluster's default-identity");
    }

    @Test
    void plaintextNeedsNoIdentity() {
      KafkafimProperties properties = ConfigTestSupport.bind(MINIMAL);

      assertThat(properties.pipelines().get("p").source().identity()).isNull();
    }

    @Test
    void sslNeedsNoIdentity() {
      assertValid(MINIMAL, K + ".security-protocol=SSL");
    }

    @Test
    void reportsInvalidDefaultIdentityOnlyOnTheCluster() {
      assertThat(
              ConfigTestSupport.errors(
                  MINIMAL,
                  SASL_SSL,
                  sasl("reader"),
                  K + ".default-identity=nope",
                  ES + ".uris=https://a:9200",
                  ES + ".identities.w.api-key=key",
                  ES + ".default-identity=nope",
                  "kafkafim.allow-inline-secrets=true"))
          .containsExactlyInAnyOrder(
              K + ".default-identity: 'nope' is not one of this cluster's identities [reader]",
              ES + ".default-identity: 'nope' is not one of this cluster's identities [w]");
    }

    @Test
    void rejectsAnUnknownSourceIdentity() {
      assertInvalid(
          MINIMAL,
          P + ".source.identity: unknown identity 'nope' on Kafka cluster 'k'",
          P + ".source.identity=nope");
    }

    @Test
    void rejectsIdentityWithoutSaslOnSaslCluster() {
      assertInvalid(
          MINIMAL,
          P
              + ".source.identity: identity 'cert' has no sasl block but Kafka cluster 'k' uses"
              + " SASL_SSL",
          SASL_SSL,
          K + ".identities.cert.ssl.keystore-location=/k",
          P + ".source.identity=cert");
    }

    @Test
    void dlqUsesItsOwnIdentityWhenSet() {
      KafkafimProperties properties =
          ConfigTestSupport.bind(
              MINIMAL,
              SASL_SSL,
              sasl("reader"),
              sasl("writer"),
              P + ".source.identity=reader",
              P + ".dlq.identity=writer");

      assertThat(properties.pipelines().get("p").dlq().identity()).isEqualTo("writer");
    }

    @Test
    void rejectsAnUnknownDlqIdentity() {
      assertInvalid(
          MINIMAL,
          P + ".dlq.identity: unknown identity 'nope' on Kafka cluster 'k'",
          P + ".dlq.identity=nope");
    }

    @Test
    void reportsAnInheritedDlqIdentityOnce() {
      assertThat(ConfigTestSupport.errors(MINIMAL, P + ".source.identity=nope")).hasSize(1);
    }

    @Test
    void routeFallsBackToTheEsClusterDefault() {
      KafkafimProperties properties =
          ConfigTestSupport.bind(
              MINIMAL, ES + ".uris=https://a:9200", basic("w"), ES + ".default-identity=w");

      assertThat(properties.pipelines().get("p").routes().get(0).identity()).isEqualTo("w");
    }

    @Test
    void failsOnHttpsWhenNoRouteIdentityResolves() {
      assertInvalid(
          MINIMAL,
          P
              + ".routes[0].identity: Elasticsearch cluster 'es' uses https and no identity"
              + " resolves",
          ES + ".uris=https://a:9200");
    }

    @Test
    void warnsOnHttpWithoutIdentity() {
      ConfigValidator.Result result = ConfigValidator.validate(ConfigTestSupport.bind(MINIMAL));

      assertThat(result.warnings())
          .contains(
              P
                  + ".routes[0]: no identity for Elasticsearch cluster 'es' over plain http;"
                  + " requests are unauthenticated (local development only)");
    }

    @Test
    void rejectsAnUnknownRouteIdentity() {
      assertInvalid(
          defaultRoute("cluster: es, index: idx, identity: nope"),
          P + ".routes[0].identity: unknown identity 'nope' on Elasticsearch cluster 'es'");
    }

    @Test
    void rejectsDuplicateIdentityNames() {
      String yaml =
          pipeline(
              """
              k:
                bootstrap-servers: [k1:9092]
                identities:
                  reader: { ssl: { keystore-location: /a } }
                  reader: { ssl: { keystore-location: /b } }
              """,
              ES_CLUSTERS,
              "source: { cluster: k, topics: [t] }\n" + ConfigTestSupport.DEFAULT_ROUTE);

      assertThat(ConfigTestSupport.runnerFailure(yaml))
          .hasStackTraceContaining("found duplicate key reader");
    }
  }

  @Nested
  class Routes {

    @Test
    void acceptsOneTrailingDefault() {
      assertValid(MINIMAL);
    }

    @Test
    void rejectsTwoDefaults() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { default: true, cluster: es, index: a }
                - { default: true, cluster: es, index: b }
              """),
          P + ".routes: route-mode first needs exactly one default: true route, found 2");
    }

    @Test
    void rejectsNoDefaultInFirstMode() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { when: [ { field: /a, exists: true } ], cluster: es, index: a }
              """),
          P + ".routes: route-mode first needs exactly one default: true route, found 0");
    }

    @Test
    void acceptsNoDefaultAndUnconditionalRoutesInAllMode() {
      assertValid(
          pipeline(
              """
              route-mode: all
              routes:
                - { cluster: es, index: a }
                - { cluster: es, index: b }
              """));
    }

    @Test
    void rejectsTwoDefaultsInAllMode() {
      assertInvalid(
          pipeline(
              """
              route-mode: all
              routes:
                - { default: true, cluster: es, index: a }
                - { default: true, cluster: es, index: b }
              """),
          P + ".routes: at most one default: true route is allowed, found 2");
    }

    @Test
    void rejectsDefaultThatIsNotLast() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { default: true, cluster: es, index: a }
                - { when: [ { field: /a, exists: true } ], cluster: es, index: b }
              """),
          P + ".routes[0].default: the default route must be the last route");
    }

    @Test
    void rejectsDefaultWithConditions() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { default: true, when: [ { field: /a, exists: true } ], cluster: es, index: a }
              """),
          P + ".routes[0].when: the default route takes no conditions");
    }

    @Test
    void rejectsAnUnconditionalRouteInFirstMode() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { cluster: es, index: a }
                - { default: true, cluster: es, index: b }
              """),
          P + ".routes[0].when: a route without conditions matches everything");
    }

    @Test
    void acceptsNamedIngestPipeline() {
      assertValid(
          pipeline(
              """
              routes:
                - { default: true, cluster: es, index: a, pipeline: orders-enrich }
              """));
    }

    @Test
    void rejectsAnEmptyIngestPipeline() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { default: true, cluster: es, index: a, pipeline: "" }
              """),
          P
              + ".routes[0].pipeline: must not be blank; omit it to index without an ingest"
              + " pipeline");
    }

    @Test
    void rejectsWhitespaceIngestPipeline() {
      assertInvalid(
          pipeline(
              """
              routes:
                - { default: true, cluster: es, index: a, pipeline: "  " }
              """),
          P
              + ".routes[0].pipeline: must not be blank; omit it to index without an ingest"
              + " pipeline");
    }

    @Test
    void rejectsValuelessIngestPipeline() {
      // Boot flattens a YAML null to "", so a bare `pipeline:` binds as blank, not as absent.
      assertInvalid(
          pipeline(
              """
              routes:
                - default: true
                  cluster: es
                  index: a
                  pipeline:
              """),
          P
              + ".routes[0].pipeline: must not be blank; omit it to index without an ingest"
              + " pipeline");
    }

    @Test
    void rejectsAnEmptyRouteList() {
      assertInvalid(pipeline("route-mode: first\n"), "pipelines[p].routes: must not be empty");
    }
  }

  @Nested
  class Conditions {

    private static String withCondition(String condition) {
      return pipeline(
          "routes:\n"
              + "  - { when: [ "
              + condition
              + " ], cluster: es, index: a }\n"
              + "  - { default: true, cluster: es, index: b }\n");
    }

    @Test
    void acceptsEachConditionShape() {
      assertValid(withCondition("{ field: /a/b/0, equals: x }"));
      assertValid(withCondition("{ field: /a~1b, in: [x] }"));
      assertValid(withCondition("{ field: /a, exists: false }"));
      assertValid(withCondition("{ field: /a, prefix: x }"));
      assertValid(withCondition("{ field: /a, matches: \"[a-z]+\\\\d\" }"));
      assertValid(withCondition("{ header: h, equals: x }"));
      assertValid(withCondition("{ topic: t }"));
    }

    @Test
    void rejectsTwoOperators() {
      assertInvalid(
          withCondition("{ field: /a, equals: x, prefix: y }"),
          P
              + ".routes[0].when[0]: a field condition needs exactly one of equals, in, exists,"
              + " prefix or matches, found equals, prefix");
    }

    @Test
    void rejectsNoOperator() {
      assertInvalid(withCondition("{ field: /a }"), "found none");
    }

    @Test
    void rejectsNoOrTwoSubjects() {
      assertInvalid(
          withCondition("{ equals: x }"),
          P + ".routes[0].when[0]: needs exactly one of field, header or topic, found 0");
      assertInvalid(
          withCondition("{ field: /a, header: h, equals: x }"),
          "needs exactly one of field, header or topic, found 2");
    }

    @Test
    void rejectsAnInvalidJsonPointer() {
      assertInvalid(
          withCondition("{ field: region, equals: eu }"),
          P + ".routes[0].when[0].field: 'region' is not a JSON pointer (must start with '/')");
      assertInvalid(
          withCondition("{ field: /a~2, equals: eu }"), "'~' must be followed by '0' or '1'");
    }

    @Test
    void rejectsAnInvalidRegex() {
      assertInvalid(
          withCondition("{ field: /a, matches: \"(unclosed\" }"),
          P + ".routes[0].when[0].matches: invalid regex: Unclosed group");
    }

    @Test
    void rejectsAnEmptyInList() {
      assertInvalid(
          withCondition("{ field: /a, in: [] }"), ".when[0].in: must list at least one value");
    }

    @Test
    void rejectsHeaderWithAnotherOperator() {
      assertInvalid(
          withCondition("{ header: h, prefix: x }"),
          "a header condition takes exactly one operator, equals");
    }

    @Test
    void rejectsTopicWithAnOperator() {
      assertInvalid(
          withCondition("{ topic: t, equals: x }"),
          "a topic condition takes no operator, found equals");
    }

    @Test
    void rejectsTopicThatIsNotSourceTopic() {
      assertInvalid(
          withCondition("{ topic: other }"),
          P + ".routes[0].when[0].topic: 'other' is not one of this pipeline's source topics");
    }
  }

  @Nested
  class IndexPatterns {

    @Test
    void acceptsLogstashPatterns() {
      assertValid(index("orders-%{region}-%{/a/b}-%{+yyyy.MM.dd}"));
    }

    @Test
    void rejectsAnUnterminatedReference() {
      assertInvalid(
          index("orders-%{region"), P + ".routes[0].index: unterminated '%{' at position 7");
    }

    @Test
    void rejectsAnInvalidDateTimeFormatter() {
      assertInvalid(
          index("orders-%{+yyyy-MM-ddTHH}"),
          P + ".routes[0].index: invalid DateTimeFormatter pattern 'yyyy-MM-ddTHH'");
    }

    @Test
    void rejectsEmptyReferencesAndForbiddenCharacters() {
      assertInvalid(index("a-%{}"), "empty reference '%{}'");
      assertInvalid(index("a-%{+}"), "empty date pattern '%{+}'");
      assertInvalid(index("a*"), "index names must not contain '*'");
      assertInvalid(index("_a"), "index names must not start with '_'");
      assertInvalid(index("a-%{/a~}"), "'%{/a~}': ");
    }

    @Test
    void rejectsColonsDotNamesAndDatesThatProduceForbiddenCharacters() {
      assertInvalid(index("orders:x"), "index names must not contain ':'");
      assertInvalid(index(".."), "index names must not be '.' or '..'");
      assertInvalid(
          index("orders-%{+yyyy/MM}"),
          P
              + ".routes[0].index: '%{+yyyy/MM}' produces '2026/12': index names must not contain"
              + " '/'");
      assertInvalid(index("orders-%{+HH:mm}"), "produces '23:59'");
    }

    @Test
    void doesNotWarnOnQuotedLiteralY() {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(defaultRoute("cluster: es, index: \"a-%{+yyyy'Y'MM}\"")));

      assertThat(result.warnings()).noneMatch(w -> w.contains("week-based year"));
    }

    @Test
    void warnsOnWeekBasedYear() {
      ConfigValidator.Result result =
          ConfigValidator.validate(ConfigTestSupport.bind(index("a-%{+YYYY.MM.dd}")));

      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).anyMatch(w -> w.contains("uses 'Y' (week-based year)"));
    }
  }

  @Nested
  class Documents {

    @Test
    void acceptsEachTimestampFormat() {
      assertValid(MINIMAL, P + ".document.timestamp-format=epoch-millis");
      assertValid(MINIMAL, P + ".document.timestamp-format=epoch-seconds");
      assertValid(MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
    }

    @Test
    void rejectsAnUnknownTimestampFormat() {
      assertInvalid(
          MINIMAL,
          P + ".document.timestamp-format: 'rfc3339' is not one of iso-8601",
          P + ".document.timestamp-format=rfc3339");
    }

    @Test
    void rejectsAnInvalidTimestampPattern() {
      assertInvalid(
          MINIMAL,
          P + ".document.timestamp-format: invalid DateTimeFormatter pattern 'yyyyT'",
          P + ".document.timestamp-format=pattern:yyyyT");
      assertInvalid(
          MINIMAL,
          "'pattern:' needs a DateTimeFormatter pattern",
          P + ".document.timestamp-format=pattern:");
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "yyyy-MM-dd HH:mm:ss",
          "yyyy-MM-dd HH:mm VV",
          "yyyyMMdd",
          "uuuu-MM-dd",
          "dd/MMM/yyyy:HH:mm:ss Z",
          "yyyy-MM-dd hh:mm a",
          "G yyyy-MM-dd",
          "yyyy-DDD",
          "YYYY-ww-e",
          "EEE, dd MMM yyyy HH:mm:ss z"
        })
    void acceptsPatternsDecodeCanParse(String pattern) {
      assertValid(MINIMAL, P + ".document.timestamp-format=pattern:" + pattern);
    }

    @ParameterizedTest
    @ValueSource(strings = {"yyyy-MM", "HH:mm", "MM-dd", "yyyy", "YYYY-MM-dd", "yyyy-ww"})
    void rejectsPatternsThatNeverYieldDates(String pattern) {
      assertInvalid(
          MINIMAL,
          P + ".document.timestamp-format: pattern '" + pattern + "' never yields a date",
          P + ".document.timestamp-format=pattern:" + pattern);
    }

    @Test
    void rejectsDayPeriodWithoutTime() {
      assertInvalid(
          MINIMAL,
          "pattern 'yyyy-MM-dd B' has a day period 'B' but no time",
          P + ".document.timestamp-format=pattern:yyyy-MM-dd B");
      assertValid(MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd h:mm B");
      // Milli-of-day carries the time without an hour letter.
      assertValid(MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd A B");
    }

    @Test
    void onlyWarnsWhenTheSampleDoesNotRoundTrip() {
      // Narrow 'S' is ambiguous for the sample's Saturday (and Sunday) but other days decode, so
      // a failed self-check must not reject the pattern.
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(
                  MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd EEEEE"));
      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).anyMatch(w -> w.contains("cannot format and parse back"));
    }

    @Test
    void doesNotWarnOnQuotedZuluLiteral() {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(
                  MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));
      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).noneMatch(w -> w.contains("timestamp-format"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"yyyy-MM-dd hh:mm", "yyyy-MM-dd mm", "yyyy-MM-dd a"})
    void rejectsPatternsThatNeverYieldCompleteTimes(String pattern) {
      assertInvalid(
          MINIMAL,
          P + ".document.timestamp-format: pattern '" + pattern + "' never yields a complete time",
          P + ".document.timestamp-format=pattern:" + pattern);
    }

    @Test
    void warnsOnPatternsWithoutOffsetOrZone() {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(
                  MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd HH:mm:ss"));
      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings())
          .anyMatch(w ->
              w.startsWith(P + ".document.timestamp-format: pattern 'yyyy-MM-dd HH:mm:ss'")
                  && w.contains("read as UTC"));
    }

    @ParameterizedTest
    @ValueSource(
        strings = {
          "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
          "yyyy-MM-dd HH:mm VV",
          "yyyy-MM-dd HH:mm z",
          "yyyy-MM-dd HH:mm O",
          "yyyy-MM-dd HH:mm Z"
        })
    void doesNotWarnWhenThePatternHasAnOffsetOrZone(String pattern) {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(MINIMAL, P + ".document.timestamp-format=pattern:" + pattern));
      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).noneMatch(w -> w.contains("timestamp-format"));
    }

    @Test
    void warnsOnTheClockHourOfDay() {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(
                  MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd kk:mmXXX"));
      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).anyMatch(w -> w.contains("uses 'k' (clock hour 1-24)"));
      ConfigValidator.Result quoted =
          ConfigValidator.validate(
              ConfigTestSupport.bind(
                  MINIMAL, P + ".document.timestamp-format=pattern:yyyy-MM-dd'k'HH:mmXXX"));
      assertThat(quoted.warnings()).noneMatch(w -> w.contains("uses 'k'"));
    }

    @Test
    void rejectsInvalidFieldPointers() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL,
              P + ".document.id-field=id",
              P + ".document.version-field=version",
              P + ".document.timestamp-field=@timestamp");

      assertThat(errors).hasSize(3);
    }

    @Test
    void acceptsAnExternalVersionWithOpIndex() {
      assertValid(MINIMAL, P + ".document.version-field=/version", P + ".document.op=index");
    }

    @Test
    void acceptsOpCreateWithoutAnExternalVersion() {
      assertValid(MINIMAL, P + ".document.op=create");
    }

    @Test
    void rejectsOpCreateWithAnExternalVersion() {
      assertThat(
              ConfigTestSupport.errors(
                  MINIMAL, P + ".document.version-field=/version", P + ".document.op=create"))
          .containsExactly(
              P
                  + ".document.op: op create does not support external versions; remove"
                  + " document.version-field or use document.op: index");
    }

    @Test
    void warnsOnAutoIds() {
      ConfigValidator.Result result =
          ConfigValidator.validate(
              ConfigTestSupport.bind(MINIMAL, P + ".document.id-missing=auto"));

      assertThat(result.warnings()).anyMatch(w -> w.startsWith(P + ".document.id-missing=auto"));
    }

    @Test
    void bindsEnumsLeniently() {
      Pipeline.Document document =
          ConfigTestSupport.bind(
                  MINIMAL,
                  P + ".document.on-bad-timestamp=ingest-time",
                  P + ".document.op=create",
                  P + ".document.on-version-conflict=dlq")
              .pipelines()
              .get("p")
              .document();

      assertThat(document.onBadTimestamp()).isEqualTo(Pipeline.OnBadTimestamp.INGEST_TIME);
      assertThat(document.op()).isEqualTo(Pipeline.Op.CREATE);
      assertThat(document.onVersionConflict()).isEqualTo(Pipeline.OnVersionConflict.DLQ);
    }
  }

  @Nested
  class Sources {

    @Test
    void rejectsBlankGroupId() {
      assertInvalid(MINIMAL, P + ".source.group-id: must not be blank", P + ".source.group-id= ");
    }

    @Test
    void rejectsDuplicateTopics() {
      assertInvalid(
          MINIMAL, P + ".source.topics[1]: 't' is listed twice", P + ".source.topics=t,t");
    }

    @Test
    void rejectsDlqTopicThatIsSourceTopic() {
      assertInvalid(
          MINIMAL,
          P + ".dlq.topic: 't' is also a source topic of this pipeline",
          P + ".dlq.topic=t");
    }

    @Test
    void rejectsRecordLimitAboveTheInFlightBudget() {
      assertInvalid(
          MINIMAL,
          P + ".source.max-record-bytes: must not exceed max-in-flight-bytes (1048576B)",
          P + ".source.max-in-flight-bytes=1MB",
          P + ".source.max-record-bytes=2MB");
    }

    @Test
    void rejectsNonPositiveDurations() {
      List<String> errors =
          ConfigTestSupport.errors(
              MINIMAL,
              P + ".source.commit-interval=0s",
              P + ".dlq.max-elapsed=-1s",
              P + ".latency.max-age=0ms");

      assertThat(errors)
          .contains(
              P + ".source.commit-interval: must be positive",
              P + ".dlq.max-elapsed: must be positive",
              P + ".latency.max-age: must be positive");
    }

    @Test
    void allowsTheSameTopicInTwoGroups() {
      assertValid(
          ConfigTestSupport.tree(
              KAFKA_CLUSTERS,
              ES_CLUSTERS,
              """
              a: { source: { cluster: k, topics: [t] }, routes: [ { default: true, cluster: es, index: a } ] }
              b: { source: { cluster: k, topics: [t] }, routes: [ { default: true, cluster: es, index: b } ] }
              """));
    }

    @Test
    void rejectsTwoPipelinesInOneConsumerGroup() {
      assertInvalid(
          ConfigTestSupport.tree(
              KAFKA_CLUSTERS,
              ES_CLUSTERS,
              """
              a: { source: { cluster: k, topics: [t], group-id: g }, routes: [ { default: true, cluster: es, index: a } ] }
              b: { source: { cluster: k, topics: [u], group-id: g }, routes: [ { default: true, cluster: es, index: b } ] }
              """),
          "kafkafim.pipelines.b.source.group-id: consumer group 'g' on Kafka cluster 'k' is"
              + " already used by pipeline 'a'");
    }

    @Test
    void warnsWhenNoPipelinesAreConfigured() {
      ConfigValidator.Result result =
          ConfigValidator.validate(new KafkafimProperties(false, null, null, null));

      assertThat(result.isValid()).isTrue();
      assertThat(result.warnings()).anyMatch(w -> w.contains("no pipelines configured"));
    }
  }

  private static String sasl(String identity) {
    return K + ".identities." + identity + ".sasl.mechanism=GSSAPI";
  }

  /** Pipeline {@code p} with a single default route made of {@code fields}. */
  private static String defaultRoute(String fields) {
    return pipeline("routes:\n  - { default: true, " + fields + " }\n");
  }

  /** Pipeline {@code p} whose only route uses index {@code pattern}. */
  private static String index(String pattern) {
    return defaultRoute("cluster: es, index: '" + pattern + "'");
  }

  /** Basic auth for {@code identity}; several properties joined with newlines. */
  private static String basic(String identity) {
    String path = ES + ".identities." + identity;
    return path + ".username=u\n" + path + ".password=pw\nkafkafim.allow-inline-secrets=true";
  }
}
