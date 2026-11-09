package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.ResponseException;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkafimApplication;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.ElasticsearchIdentity;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchClients;
import io.github.matabar.kafkafim.source.kafka.KafkaPipelineSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Hot reload end to end (DESIGN §5.6): the application reads its pipelines from a config directory
 * and its Elasticsearch passwords from a configtree, both written the way kubelet updates mounted
 * volumes (a new {@code ..timestamp} directory and an atomic {@code ..data} symlink swap), against
 * a real broker and a secured Elasticsearch 8.19.
 */
class HotReloadIntegrationTest {

  private static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  private static final String ADMIN_PASSWORD = "admin-pw-7f1e";

  private static final ElasticsearchContainer ES =
      new ElasticsearchContainer(
              DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
          .withPassword(ADMIN_PASSWORD)
          .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
          // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
          .withStartupTimeout(Duration.ofMinutes(3));

  private static final Duration TIMEOUT = Duration.ofSeconds(90);
  private static final JsonMapper JSON = JsonMapper.builder().build();

  @TempDir static Path shared;
  private static Path caFile;
  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;
  private static Rest5Client es;
  private static HttpClient http;

  @TempDir Path dir;
  private final List<ConfigurableApplicationContext> contexts = new ArrayList<>();
  private final AtomicInteger volumeGeneration = new AtomicInteger();

  @BeforeAll
  static void startContainers() throws Exception {
    Startables.deepStart(KAFKA, ES).join();
    caFile = Files.write(shared.resolve("ca.crt"), ES.caCertAsBytes().orElseThrow());
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    Map<String, Object> producerConfig = new HashMap<>();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    producerConfig.put(ProducerConfig.LINGER_MS_CONFIG, 5);
    producer = new KafkaProducer<>(producerConfig);
    es =
        ElasticsearchClients.create(
            "it",
            cluster(
                Map.of("admin", new ElasticsearchIdentity("elastic", ADMIN_PASSWORD, null, null))),
            "admin",
            ClientSettings.DEFAULT);
    http = HttpClient.newHttpClient();
  }

  @AfterAll
  static void stopContainers() throws IOException {
    if (producer != null) {
      producer.close();
    }
    if (admin != null) {
      admin.close();
    }
    if (es != null) {
      es.close();
    }
    if (http != null) {
      http.close();
    }
    ES.stop();
    KAFKA.stop();
  }

  @AfterEach
  void closeContexts() {
    contexts.forEach(ConfigurableApplicationContext::close);
  }

  private static ElasticsearchCluster cluster(Map<String, ElasticsearchIdentity> identities) {
    return new ElasticsearchCluster(
        List.of("https://" + ES.getHttpHostAddress()),
        new ElasticsearchCluster.Trust(caFile.toString()),
        identities,
        null,
        null,
        true,
        null);
  }

  // ---- the watcher path ----

  @Test
  void watcherAppliesPipelineChangesRejectsInvalidTreesAndRotatesSecrets() throws Exception {
    createTopics("wa.events", "wb.events", "wc.events");
    createUser("kfm-wa", "pass-a-1");
    createUser("kfm-wb", "pass-b-1");
    writeSecret("writer-a", "kfm-wa", "pass-a-1");
    writeSecret("writer-b", "kfm-wb", "pass-b-1");
    String a = pipeline("a", "wa.events", "writer-a", "wa-1");
    writeConfig(true, a);
    ConfigurableApplicationContext app = start();
    PipelineSupervisor supervisor = app.getBean(PipelineSupervisor.class);
    final MeterRegistry meters = app.getBean(MeterRegistry.class);
    awaitRunning(supervisor, "a");

    sendDocs("wa.events", "a", 0, 20);
    awaitCommittedToEnd("kafkafim.a", "wa.events");
    assertThat(count("wa-1")).isEqualTo(20);
    final KafkaPipelineSource a1 = supervisor.source("a");

    // 1. A's index changes and B and C appear: A drains and restarts, B and C start.
    String changedA = pipeline("a", "wa.events", "writer-a", "wa-2");
    String b = pipeline("b", "wb.events", "writer-b", "wb");
    String c = pipeline("c", "wc.events", "writer-b", "wc");
    writeConfig(true, changedA + b + c);
    await().atMost(TIMEOUT).until(() -> generation(meters) == 2);
    awaitRunning(supervisor, "a", "b", "c");
    KafkaPipelineSource a2 = supervisor.source("a");
    assertThat(a2).isNotSameAs(a1);
    // No loss and no re-delivery: offsets continue where the first run committed.
    sendDocs("wa.events", "a", 20, 40);
    sendDocs("wb.events", "b", 0, 10);
    awaitCommittedToEnd("kafkafim.a", "wa.events");
    awaitCommittedToEnd("kafkafim.b", "wb.events");
    assertThat(count("wa-1")).isEqualTo(20);
    assertThat(count("wa-2")).isEqualTo(20);
    assertThat(count("wb")).isEqualTo(10);
    assertThat(gauge(meters, "c", "running")).isEqualTo(1);
    final KafkaPipelineSource b1 = supervisor.source("b");
    final KafkaPipelineSource c1 = supervisor.source("c");

    // 2. An invalid tree (B routes to an unknown cluster) is rejected whole.
    writeConfig(
        true, changedA + b.replace("cluster: local, identity", "cluster: nope, identity") + c);
    await().atMost(TIMEOUT).until(() -> reloads(meters, "failure") == 1);
    assertThat(generation(meters)).isEqualTo(2);
    assertThat(supervisor.states().values()).containsOnly(PipelineState.RUNNING);
    assertThat(supervisor.source("a")).isSameAs(a2);
    assertThat(supervisor.source("b")).isSameAs(b1);
    assertThat(supervisor.source("c")).isSameAs(c1);

    // 3. C is deleted: it drains, leaves its group and loses its gauges; A and B are untouched.
    writeConfig(true, changedA + b);
    await().atMost(TIMEOUT).until(() -> generation(meters) == 3);
    assertThat(supervisor.pipelineNames()).containsExactly("a", "b");
    assertThat(supervisor.source("a")).isSameAs(a2);
    assertThat(supervisor.source("b")).isSameAs(b1);
    assertThat(meters.find(PipelineStateMetrics.NAME).tag("pipeline", "c").gauges()).isEmpty();
    assertThat(
            admin
                .describeConsumerGroups(List.of("kafkafim.c"))
                .all()
                .get()
                .get("kafkafim.c")
                .members())
        .isEmpty();

    // 4. B's password is rotated in Elasticsearch, then in its Secret: only B restarts.
    changePassword("kfm-wb", "pass-b-2");
    writeSecret("writer-b", "kfm-wb", "pass-b-2");
    await().atMost(TIMEOUT).until(() -> generation(meters) == 4);
    awaitRunning(supervisor, "a", "b");
    assertThat(supervisor.source("a")).isSameAs(a2);
    assertThat(supervisor.source("b")).isNotSameAs(b1);
    sendDocs("wb.events", "b", 10, 25);
    awaitCommittedToEnd("kafkafim.b", "wb.events");
    assertThat(count("wb")).isEqualTo(25);
    assertThat(reloads(meters, "success")).isEqualTo(3);
  }

  // ---- the endpoint ----

  @Test
  void endpointReloadsOnDemandAndReportsWithoutSecrets() throws Exception {
    createTopics("ea.events", "eb.events");
    createUser("kfm-ea", "ea-secret-1");
    writeSecret("writer-a", "kfm-ea", "ea-secret-1");
    writeSecret("writer-b", "kfm-ea", "ea-secret-1");
    String e = pipeline("e", "ea.events", "writer-a", "ea-1");
    writeConfig(false, e); // watcher off: only the endpoint reloads
    ConfigurableApplicationContext app = start();
    PipelineSupervisor supervisor = app.getBean(PipelineSupervisor.class);
    awaitRunning(supervisor, "e");

    HttpResponse<String> unchanged = postReload(app);
    assertThat(unchanged.statusCode()).isEqualTo(200);
    assertThat(JSON.readTree(unchanged.body()).get("result").asString()).isEqualTo("unchanged");

    writeConfig(
        false,
        pipeline("e", "ea.events", "writer-a", "ea-2")
            + pipeline("f", "eb.events", "writer-b", "eb"));
    HttpResponse<String> applied = postReload(app);
    assertThat(applied.statusCode()).isEqualTo(200);
    JsonNode body = JSON.readTree(applied.body());
    assertThat(body.get("result").asString()).isEqualTo("applied");
    assertThat(body.get("generation").asLong()).isEqualTo(2);
    assertThat(body.get("added").toString()).isEqualTo("[\"f\"]");
    assertThat(body.get("restarted").get(0).get("pipeline").asString()).isEqualTo("e");
    assertThat(body.get("restarted").get(0).get("reasons").toString())
        .contains("pipeline definition changed");
    awaitRunning(supervisor, "e", "f");

    writeConfig(
        false,
        pipeline("e", "ea.events", "writer-a", "ea-2").replace("index: ea-2", "index: \"\""));
    HttpResponse<String> rejected = postReload(app);
    assertThat(rejected.statusCode()).isEqualTo(400);
    JsonNode errors = JSON.readTree(rejected.body());
    assertThat(errors.get("result").asString()).isEqualTo("rejected");
    assertThat(errors.get("generation").asLong()).isEqualTo(2);
    assertThat(errors.get("errors").toString()).contains("index");
    assertThat(supervisor.pipelineNames()).containsExactly("e", "f");

    // A rotated secret restarts the pipelines using it; the summary names, never shows, it.
    writeConfig(
        false,
        pipeline("e", "ea.events", "writer-a", "ea-2")
            + pipeline("f", "eb.events", "writer-b", "eb"));
    changePassword("kfm-ea", "ea-secret-2");
    writeSecret("writer-a", "kfm-ea", "ea-secret-2");
    HttpResponse<String> rotated = postReload(app);
    assertThat(rotated.statusCode()).isEqualTo(200);
    assertThat(rotated.body())
        .contains("Elasticsearch identity 'local/writer-a' changed")
        .doesNotContain("ea-secret");
    assertThat(JSON.readTree(rotated.body()).get("unchanged").toString()).isEqualTo("[\"f\"]");
    awaitRunning(supervisor, "e", "f");
  }

  // ---- application and files ----

  private ConfigurableApplicationContext start() {
    Map<String, Object> props = new LinkedHashMap<>();
    props.put("spring.docker.compose.enabled", false);
    props.put("management.otlp.metrics.export.enabled", false);
    props.put("management.tracing.sampling.probability", 0.0);
    props.put("spring.config.additional-location", "file:" + dir.resolve("config") + "/");
    props.put("spring.config.import", "optional:configtree:" + dir.resolve("secrets") + "/");
    ConfigurableApplicationContext context =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .profiles("reload-it") // reads application-reload-it.yaml; not `local`
            .properties(props)
            // application.yaml sets 8080; a command-line argument beats it (defaults do not).
            .run("--server.port=0");
    contexts.add(context);
    return context;
  }

  private static String pipeline(String name, String topic, String identity, String index) {
    String yaml =
        """
        %s:
          source: { cluster: local, topics: [%s], offset-reset: earliest, commit-interval: 200ms }
          document: { id-field: /id }
          routes: [ { default: true, cluster: local, identity: %s, index: %s } ]
        """;
    return yaml.formatted(name, topic, identity, index);
  }

  /** Writes {@code application-reload-it.yaml} the way kubelet updates a ConfigMap volume. */
  private void writeConfig(boolean watch, String pipelines) throws IOException {
    String yaml =
        """
        kafkafim:
          reload: { enabled: %s, poll-interval: 300ms, debounce: 200ms }
          kafka:
            clusters:
              local: { bootstrap-servers: ["%s"] }
          elasticsearch:
            clusters:
              local:
                uris: ["https://%s"]
                ssl: { ca-certificate: "%s" }
                bulk: { max-age: 100ms }
                identities:
                  writer-a:
                    username: "${elasticsearch.local.writer-a.username}"
                    password: "${elasticsearch.local.writer-a.password}"
                  writer-b:
                    username: "${elasticsearch.local.writer-b.username}"
                    password: "${elasticsearch.local.writer-b.password}"
          pipelines:
        %s
        """
            .formatted(
                watch,
                KAFKA.getBootstrapServers(),
                ES.getHttpHostAddress(),
                caFile,
                pipelines.indent(4));
    kubeletWrite(dir.resolve("config"), Map.of("application-reload-it.yaml", yaml));
  }

  /** Writes one identity's Secret volume, mounted under the configtree import. */
  private void writeSecret(String identity, String username, String password) throws IOException {
    kubeletWrite(
        dir.resolve("secrets/elasticsearch/local." + identity),
        Map.of("username", username, "password", password));
  }

  private void kubeletWrite(Path volume, Map<String, String> files) throws IOException {
    Files.createDirectories(volume);
    Path data = volume.resolve("..2026_11_08_" + volumeGeneration.incrementAndGet());
    Files.createDirectories(data);
    for (Map.Entry<String, String> file : files.entrySet()) {
      Files.writeString(data.resolve(file.getKey()), file.getValue());
    }
    Path tmp = volume.resolve("..data_tmp");
    Files.deleteIfExists(tmp);
    Files.createSymbolicLink(tmp, data.getFileName());
    Files.move(tmp, volume.resolve("..data"), StandardCopyOption.ATOMIC_MOVE);
    for (String name : files.keySet()) {
      Path link = volume.resolve(name);
      if (!Files.isSymbolicLink(link)) {
        Files.createSymbolicLink(link, Path.of("..data", name));
      }
    }
  }

  private static HttpResponse<String> postReload(ConfigurableApplicationContext app)
      throws IOException, InterruptedException {
    String port = app.getEnvironment().getProperty("local.server.port");
    return http.send(
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/actuator/reload"))
            .POST(HttpRequest.BodyPublishers.noBody())
            .timeout(TIMEOUT)
            .build(),
        HttpResponse.BodyHandlers.ofString());
  }

  // ---- assertions ----

  private static void awaitRunning(PipelineSupervisor supervisor, String... pipelines) {
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> {
          for (String pipeline : pipelines) {
            assertThat(supervisor.state(pipeline)).isEqualTo(PipelineState.RUNNING);
          }
        });
  }

  private static double generation(MeterRegistry meters) {
    return meters.get(ConfigReloader.GENERATION).gauge().value();
  }

  private static double reloads(MeterRegistry meters, String result) {
    return meters.get(ConfigReloader.RELOADS).tag("result", result).counter().count();
  }

  private static double gauge(MeterRegistry meters, String pipeline, String state) {
    return meters
        .get(PipelineStateMetrics.NAME)
        .tag("pipeline", pipeline)
        .tag("state", state)
        .gauge()
        .value();
  }

  // ---- Kafka ----

  private static void createTopics(String... topics) throws Exception {
    List<NewTopic> newTopics = new ArrayList<>();
    for (String topic : topics) {
      newTopics.add(new NewTopic(topic, 3, (short) 1));
      newTopics.add(new NewTopic(topic + ".kafkafim-dlq", 1, (short) 1));
    }
    admin.createTopics(newTopics).all().get();
  }

  private static void sendDocs(String topic, String prefix, int from, int to) {
    for (int i = from; i < to; i++) {
      producer.send(
          new ProducerRecord<>(
              topic, bytes("k" + i), bytes("{\"id\":\"" + prefix + "-" + i + "\"}")));
    }
    producer.flush();
  }

  private static void awaitCommittedToEnd(String group, String topic) {
    await()
        .atMost(TIMEOUT)
        .untilAsserted(() -> assertThat(committedTotal(group, topic)).isEqualTo(endTotal(topic)));
  }

  private static long endTotal(String topic) throws Exception {
    Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
    admin
        .describeTopics(List.of(topic))
        .allTopicNames()
        .get()
        .get(topic)
        .partitions()
        .forEach(p -> latest.put(new TopicPartition(topic, p.partition()), OffsetSpec.latest()));
    return admin.listOffsets(latest).all().get().values().stream()
        .mapToLong(info -> info.offset())
        .sum();
  }

  private static long committedTotal(String group, String topic) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> committed =
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
    return committed.entrySet().stream()
        .filter(e -> e.getKey().topic().equals(topic) && e.getValue() != null)
        .mapToLong(e -> e.getValue().offset())
        .sum();
  }

  private static byte[] bytes(String text) {
    return text.getBytes(StandardCharsets.UTF_8);
  }

  // ---- Elasticsearch ----

  private static void createUser(String user, String password) throws IOException {
    call(
        "PUT",
        "/_security/user/" + user,
        "{\"password\":\"" + password + "\",\"roles\":[\"superuser\"]}");
  }

  private static void changePassword(String user, String password) throws IOException {
    call("POST", "/_security/user/" + user + "/_password", "{\"password\":\"" + password + "\"}");
  }

  private static long count(String index) throws IOException {
    call("POST", "/" + index + "/_refresh?ignore_unavailable=true&allow_no_indices=true", null);
    JsonNode response =
        call("GET", "/" + index + "/_count?ignore_unavailable=true&allow_no_indices=true", null);
    return response.get("count").asLong();
  }

  private static JsonNode call(String method, String path, String json) throws IOException {
    Request request = new Request(method, path);
    if (json != null) {
      request.setJsonEntity(json);
    }
    try {
      return JSON.readTree(
          EntityUtils.toString(es.performRequest(request).getEntity(), StandardCharsets.UTF_8));
    } catch (ResponseException e) {
      throw new AssertionError(method + " " + path + " failed: " + e.getMessage(), e);
    } catch (org.apache.hc.core5.http.ParseException e) {
      throw new IOException(e);
    }
  }
}
