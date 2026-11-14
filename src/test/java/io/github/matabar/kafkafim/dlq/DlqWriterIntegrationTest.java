package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * {@link DlqWriter} against a real broker with topic auto-creation disabled: byte-for-byte round
 * trip with every header, completion only after the acknowledgement, and a missing DLQ topic
 * surfacing as a configuration failure without completing and without being created (DESIGN §7, §9;
 * ADR-0011, ADR-0021).
 */
@Testcontainers
class DlqWriterIntegrationTest {

  @Container
  static final KafkaContainer KAFKA =
      KafkaTestContainers.kafka().withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

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
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
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

  private DlqWriter writer(String sourceTopic) {
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    Pipeline pipeline =
        new Pipeline(
            new Pipeline.Source(
                "local", List.of(sourceTopic), null, null, null, null, null, null, null),
            null,
            null,
            null,
            null,
            null);
    producers =
        new DlqProducers(
            new KafkafimProperties(
                false,
                new KafkafimProperties.Kafka(Map.of("local", local)),
                null,
                Map.of("orders", pipeline)),
            events,
            OPTIONS);
    return producers.writer("orders");
  }

  private static String uniqueTopic() {
    return "orders-" + UUID.randomUUID();
  }

  private static void createTopic(String topic) throws Exception {
    KafkaTopics.create(admin, List.of(new NewTopic(topic, 1, (short) 1)));
  }

  private static Header header(String key, byte[] value) {
    return new RecordHeader(key, value);
  }

  @Test
  void roundTripsKeyValueAndHeadersByteForByte() throws Exception {
    String source = uniqueTopic();
    DlqWriter writer = writer(source);
    createTopic(writer.topic());
    byte[] key = {0, 1, (byte) 0xfe, (byte) 0xff};
    byte[] value = new byte[256];
    for (int i = 0; i < value.length; i++) {
      value[i] = (byte) i; // every byte value, not valid UTF-8
    }
    List<Header> original =
        List.of(
            header(
                "traceparent",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes()),
            header("bin", new byte[] {(byte) 0xc3, 0x28}),
            header("dup", "1".getBytes()),
            header("dup", "2".getBytes()),
            header("none", null),
            header("kafkafim.error.type", "stale".getBytes()));
    Instant firstFailure = Instant.parse("2026-11-08T10:15:30.123Z");
    DlqRequest request =
        new DlqRequest(
            source,
            2,
            987,
            Instant.parse("2026-11-01T00:00:00Z"),
            key,
            value,
            original,
            "es_error",
            "é".repeat(3000),
            "search-eu",
            "orders-eu-2026.11",
            3,
            firstFailure,
            5);
    long before = System.currentTimeMillis();

    RecordMetadata metadata = writer.send(request).get(60, TimeUnit.SECONDS);

    ConsumerRecord<byte[], byte[]> record = readOne(writer.topic(), metadata.offset());
    assertThat(record.key()).isEqualTo(key);
    assertThat(record.value()).isEqualTo(value);
    assertThat(record.timestamp()).isBetween(before - 1000, System.currentTimeMillis() + 1000);
    List<Header> expected = new ArrayList<>(original.subList(0, 5));
    DlqRecordFormat.headers("orders", request)
        .forEach(h -> {
          if (DlqRecordFormat.isReserved(h.key())) {
            expected.add(h);
          }
        });
    assertThat(headerStrings(record.headers())).isEqualTo(headerStrings(expected));
    assertThat(record.headers().headers("kafkafim.error.type")).hasSize(1);
    assertThat(record.headers().lastHeader("kafkafim.error.reason").value()).hasSize(4096);
    assertThat(new String(record.headers().lastHeader("kafkafim.doc-index").value()))
        .isEqualTo("5");
    assertThat(events.written).containsExactly(metadata.offset());
    assertThat(events.failures).isEmpty();
  }

  @Test
  void keepsNullKeyAndNullValue() throws Exception {
    DlqWriter writer = writer(uniqueTopic());
    createTopic(writer.topic());
    DlqRequest tombstone =
        new DlqRequest(
            "orders.v2",
            0,
            1,
            null,
            null,
            null,
            List.of(),
            "decode",
            "tombstone",
            null,
            null,
            1,
            Instant.now(),
            null);

    RecordMetadata metadata = writer.send(tombstone).get(60, TimeUnit.SECONDS);

    ConsumerRecord<byte[], byte[]> record = readOne(writer.topic(), metadata.offset());
    assertThat(record.key()).isNull();
    assertThat(record.value()).isNull();
    assertThat(new String(record.headers().lastHeader("kafkafim.error.type").value()))
        .isEqualTo("decode");
  }

  @Test
  void missingTopicIsConfigurationFailureCompletingOnlyOnceTheTopicExists() throws Exception {
    DlqWriter writer = writer(uniqueTopic());
    assertThat(producers.checkTopic("orders")).isEqualTo(TopicStatus.MISSING);

    CompletableFuture<RecordMetadata> result = writer.send(request());

    await().atMost(TIMEOUT).until(() -> events.failures.size() >= 3);
    assertThat(events.failures)
        .allSatisfy(failure -> {
          assertThat(failure.kind()).isEqualTo(DlqFailure.Kind.CONFIGURATION);
          assertThat(failure.cause()).isInstanceOf(DlqTopicException.class);
        });
    assertThat(events.blocked).hasValue(1);
    assertThat(writer.isBlocked()).isTrue();
    assertThat(result).isNotDone();
    assertThat(admin.listTopics().names().get()).doesNotContain(writer.topic());

    createTopic(writer.topic());

    RecordMetadata metadata = result.get(60, TimeUnit.SECONDS);
    assertThat(readOne(writer.topic(), metadata.offset()).value()).isEqualTo(request().value());
    await().atMost(TIMEOUT).until(() -> !writer.isBlocked());
    assertThat(events.unblocked).hasValue(1);
    assertThat(producers.checkTopic("orders")).isEqualTo(TopicStatus.EXISTS);
  }

  @Test
  void closingWhileBlockedCompletesWithDlqClosedException() {
    DlqWriter writer = writer(uniqueTopic());
    CompletableFuture<RecordMetadata> result = writer.send(request());
    await().atMost(TIMEOUT).until(() -> !events.failures.isEmpty());

    producers.close();

    assertThatThrownBy(() -> result.get(30, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(DlqClosedException.class);
  }

  private static DlqRequest request() {
    return new DlqRequest(
        "orders.v2",
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
    try (KafkaConsumer<byte[], byte[]> consumer =
        new KafkaConsumer<>(
            Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class))) {
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

  private static List<String> headerStrings(Iterable<Header> headers) {
    List<String> out = new ArrayList<>();
    for (Header header : headers) {
      out.add(
          header.key()
              + "="
              + (header.value() == null ? "<null>" : HexFormat.of().formatHex(header.value())));
    }
    return out;
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
