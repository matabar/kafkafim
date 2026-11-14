package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The DLQ producer against a broker that enforces ACLs (SASL/PLAIN, {@code StandardAuthorizer}): an
 * identity with {@code Describe} but no {@code Write} ACL fails the idempotent producer's {@code
 * InitProducerId} with {@code CLUSTER_AUTHORIZATION_FAILED}. The send must be retried as a
 * configuration failure with the pipeline blocked, and must complete without a restart once the ACL
 * is granted (DESIGN §7 "DLQ produce fails", §9; ADR-0011). {@code KafkaDlqClientTest} covers the
 * producer replacement this error triggers with {@code MockProducer}; here a real broker drives
 * that path.
 *
 * <p>kafka-clients 4.2 itself treats this error as abortable, not fatal, for an idempotent
 * producer: the same producer would succeed after the grant. So the test asserts what the writer
 * guarantees (classification, blocking, completion after the grant), not that the producer was
 * replaced. kafka-clients still logs "Aborting producer batches due to fatal error" on this path,
 * and {@code KafkaDlqClient} that the producer may be in a fatal state; the producer is not fatal.
 * A send that fails because of the replacement (its batch aborted by the close) still counts as
 * {@code CONFIGURATION}, by the failure that triggered it.
 */
@Testcontainers
class DlqWriterAclIntegrationTest {

  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "admin-secret";
  private static final String DLQ_USER = "dlq";
  private static final String DLQ_PASSWORD = "dlq-secret";

  /**
   * The client-facing {@code PLAINTEXT} listener of {@link KafkaTestContainers#jvmKafka} (the
   * native image cannot do SASL) speaks SASL/PLAIN; the broker and controller listeners stay
   * plaintext, so their {@code ANONYMOUS} principal is a super user next to {@code admin}.
   */
  @Container
  static final KafkaContainer KAFKA =
      KafkaTestContainers.jvmKafka()
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
          .withEnv(
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
              "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
              "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " user_"
                  + ADMIN
                  + "=\""
                  + ADMIN_PASSWORD
                  + "\" user_"
                  + DLQ_USER
                  + "=\""
                  + DLQ_PASSWORD
                  + "\";")
          .withEnv(
              "KAFKA_AUTHORIZER_CLASS_NAME",
              "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
          .withEnv("KAFKA_SUPER_USERS", "User:" + ADMIN + ";User:ANONYMOUS");

  private static final Duration TIMEOUT = Duration.ofSeconds(60);
  private static final DlqOptions OPTIONS =
      new DlqOptions(
          Duration.ofMillis(50),
          Duration.ofMillis(500),
          Duration.ofSeconds(5),
          Duration.ofSeconds(5));

  private static Admin admin;

  private final Events events = new Events();
  private DlqProducers producers;

  @BeforeAll
  static void connect() {
    admin = Admin.create(adminSettings());
  }

  @AfterAll
  static void disconnect() {
    admin.close();
  }

  @AfterEach
  void closeProducers() {
    if (producers != null) {
      producers.close();
    }
  }

  @Test
  void missingWriteAclBlocksAsConfigurationFailureAndGrantingItCompletesTheSameSend()
      throws Exception {
    String source = "orders-" + UUID.randomUUID();
    DlqWriter writer = writer(source);
    KafkaTopics.create(admin, List.of(new NewTopic(writer.topic(), 1, (short) 1)));
    assertThat(producers.checkTopic("orders")).isEqualTo(TopicStatus.NOT_AUTHORIZED);
    grant(writer.topic(), AclOperation.DESCRIBE);
    await().atMost(TIMEOUT).until(() -> producers.checkTopic("orders") == TopicStatus.EXISTS);

    CompletableFuture<RecordMetadata> result = writer.send(request(source));

    // Two failures: the retry after the first one fails the same way.
    await().atMost(TIMEOUT).until(() -> events.failures.size() >= 2);
    assertThat(events.failures)
        .allSatisfy(failure -> {
          assertThat(failure.kind()).isEqualTo(DlqFailure.Kind.CONFIGURATION);
          assertThat(causes(failure.cause()))
              .hasAtLeastOneElementOfType(ClusterAuthorizationException.class);
        });
    // One report per attempt: kafka-clients both throws and later fails the appended batch of a
    // send it rejects in its error state, and only one of the two may reach the writer.
    assertThat(events.failures)
        .extracting(DlqFailure::attempt)
        .as("retries of the one send")
        .startsWith(1, 2)
        .doesNotHaveDuplicates();
    assertThat(events.blocked).hasValue(1);
    assertThat(writer.isBlocked()).isTrue();
    assertThat(result).isNotDone();

    grant(writer.topic(), AclOperation.WRITE);

    RecordMetadata metadata = result.get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    assertThat(readOne(writer.topic(), metadata.offset()).value())
        .isEqualTo(request(source).value());
    // onWritten and onUnblocked run before the future completes, so no wait is needed.
    assertThat(writer.isBlocked()).isFalse();
    assertThat(events.unblocked).hasValue(1);
    assertThat(events.written).containsExactly(metadata.offset());
  }

  private DlqWriter writer(String sourceTopic) {
    KafkaCluster cluster =
        new KafkaCluster(
            List.of(KAFKA.getBootstrapServers()),
            KafkaCluster.SecurityProtocol.SASL_PLAINTEXT,
            null,
            Map.of(
                DLQ_USER,
                new KafkaCluster.KafkaIdentity(
                    new KafkaCluster.Sasl("PLAIN", DLQ_USER, DLQ_PASSWORD), null)),
            DLQ_USER,
            null);
    Pipeline pipeline =
        new Pipeline(
            new Pipeline.Source(
                "local", List.of(sourceTopic), null, null, null, null, null, null, null),
            null,
            null,
            null,
            new Pipeline.Dlq(null, DLQ_USER, null, null),
            null);
    producers =
        new DlqProducers(
            new KafkafimProperties(
                false,
                new KafkafimProperties.Kafka(Map.of("local", cluster)),
                null,
                Map.of("orders", pipeline)),
            events,
            OPTIONS);
    return producers.writer("orders");
  }

  private static void grant(String topic, AclOperation operation) throws Exception {
    admin
        .createAcls(
            List.of(
                new AclBinding(
                    new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL),
                    new AccessControlEntry(
                        "User:" + DLQ_USER, "*", operation, AclPermissionType.ALLOW))))
        .all()
        .get(30, TimeUnit.SECONDS);
  }

  private static Map<String, Object> adminSettings() {
    return Map.of(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers(),
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        "SASL_PLAINTEXT",
        SaslConfigs.SASL_MECHANISM,
        "PLAIN",
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
            + ADMIN
            + "\" password=\""
            + ADMIN_PASSWORD
            + "\";");
  }

  private static DlqRequest request(String source) {
    return new DlqRequest(
        source,
        0,
        1,
        null,
        "k".getBytes(StandardCharsets.UTF_8),
        "{\"broken\"".getBytes(StandardCharsets.UTF_8),
        List.of(),
        "decode",
        "unexpected end of input",
        null,
        null,
        1,
        Instant.now(),
        null);
  }

  private static ConsumerRecord<byte[], byte[]> readOne(String topic, long offset) {
    Map<String, Object> settings = new HashMap<>(adminSettings());
    settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(settings)) {
      TopicPartition partition = new TopicPartition(topic, 0);
      consumer.assign(List.of(partition));
      consumer.seek(partition, offset);
      long deadline = System.nanoTime() + TIMEOUT.toNanos();
      while (System.nanoTime() < deadline) {
        for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
          if (record.offset() == offset) {
            return record;
          }
        }
      }
      throw new AssertionError("no record at " + topic + "@" + offset);
    }
  }

  private static List<Throwable> causes(Throwable failure) {
    List<Throwable> chain = new ArrayList<>();
    for (Throwable t = failure; t != null && !chain.contains(t); t = t.getCause()) {
      chain.add(t);
    }
    return chain;
  }

  /** Listener calls, for assertions. */
  private static final class Events implements DlqListener {
    final List<Long> written = new CopyOnWriteArrayList<>();
    final List<DlqFailure> failures = new CopyOnWriteArrayList<>();
    final AtomicInteger blocked = new AtomicInteger();
    final AtomicInteger unblocked = new AtomicInteger();

    @Override
    public void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {
      written.add(metadata.offset());
    }

    @Override
    public void onSendFailed(String pipeline, DlqRequest request, DlqFailure failure) {
      failures.add(failure);
    }

    @Override
    public void onBlocked(String pipeline, DlqFailure failure) {
      blocked.incrementAndGet();
    }

    @Override
    public void onUnblocked(String pipeline) {
      unblocked.incrementAndGet();
    }
  }
}
