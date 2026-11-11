package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DlqWriterTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final DlqOptions FAST =
      new DlqOptions(
          Duration.ofMillis(5),
          Duration.ofMillis(20),
          Duration.ofSeconds(1),
          Duration.ofSeconds(2));

  private final List<FakeClient> clients = new CopyOnWriteArrayList<>();
  private final Events events = new Events();
  private DlqProducers producers;

  @AfterEach
  void close() {
    if (producers != null) {
      producers.close();
    }
  }

  static KafkafimProperties properties(Map<String, Pipeline> pipelines) {
    KafkaCluster.KafkaIdentity identity = new KafkaCluster.KafkaIdentity(null, null);
    KafkaCluster cluster =
        new KafkaCluster(
            List.of("localhost:9092"),
            null,
            null,
            Map.of("reader", identity, "writer", identity),
            null,
            null);
    return new KafkafimProperties(
        false, new KafkafimProperties.Kafka(Map.of("local", cluster)), null, pipelines);
  }

  static Pipeline pipeline(String topic, String sourceIdentity, String dlqIdentity) {
    return new Pipeline(
        new Pipeline.Source(
            "local", List.of(topic), null, sourceIdentity, null, null, null, null, null),
        null,
        null,
        null,
        new Pipeline.Dlq(null, dlqIdentity, null, null),
        null);
  }

  private DlqProducers producers(Map<String, Pipeline> pipelines) {
    producers =
        new DlqProducers(
            properties(pipelines),
            events,
            FAST,
            settings -> {
              FakeClient client = new FakeClient(settings);
              clients.add(client);
              return client;
            });
    return producers;
  }

  private DlqWriter ordersWriter() {
    return producers(Map.of("orders", pipeline("orders.v2", "reader", null))).writer("orders");
  }

  private static DlqRequest request() {
    return new DlqRequest(
        "orders.v2",
        0,
        5,
        null,
        new byte[] {1},
        new byte[] {2},
        List.of(),
        "decode",
        "bad json",
        null,
        null,
        1,
        Instant.now(),
        null);
  }

  @Test
  void completesOnlyAfterTheBrokerAcknowledged() throws Exception {
    DlqWriter writer = ordersWriter();

    final CompletableFuture<RecordMetadata> result = writer.send(request());

    FakeClient client = clients.get(0);
    PendingSend sent = client.next();
    assertThat(sent.record().topic()).isEqualTo("orders.v2.kafkafim-dlq");
    assertThat(sent.record().key()).containsExactly(1);
    Thread.sleep(50);
    assertThat(result).isNotDone();

    sent.ack(17);

    assertThat(result.get(10, TimeUnit.SECONDS).offset()).isEqualTo(17);
    assertThat(events.log).containsExactly("written orders 17");
    assertThat(writer.isBlocked()).isFalse();
    assertThat(client.describes.get()).isEqualTo(1);
  }

  @Test
  void transientFailureIsRetriedAndBlocksUntilAcknowledged() throws Exception {
    DlqWriter writer = ordersWriter();
    CompletableFuture<RecordMetadata> result = writer.send(request());
    FakeClient client = clients.get(0);

    client.next().fail(new NetworkException("broker gone"));
    client.next().fail(new NetworkException("broker still gone"));
    PendingSend third = client.next();

    assertThat(result).isNotDone();
    assertThat(writer.isBlocked()).isTrue();
    third.ack(3);
    result.get(10, TimeUnit.SECONDS);
    await().atMost(TIMEOUT).until(() -> !writer.isBlocked());
    assertThat(events.log)
        .containsExactly(
            "failed orders TRANSIENT 1",
            "blocked orders TRANSIENT",
            "failed orders TRANSIENT 2",
            "written orders 3",
            "unblocked orders");
  }

  @Test
  void missingTopicIsConfigurationFailureAndNeverReachesTheProducer() throws Exception {
    DlqWriter writer = ordersWriter();
    FakeClient client = clients.get(0);
    client.topic.set(TopicStatus.MISSING);

    CompletableFuture<RecordMetadata> result = writer.send(request());

    await().atMost(TIMEOUT).until(() -> client.describes.get() >= 2);
    assertThat(result).isNotDone();
    assertThat(client.sends).isEmpty();
    assertThat(writer.isBlocked()).isTrue();
    assertThat(events.failures.get(0).kind()).isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(events.failures.get(0).cause())
        .isInstanceOf(DlqTopicException.class)
        .hasMessageContaining("orders.v2.kafkafim-dlq")
        .hasMessageContaining("does not exist")
        .hasMessageContaining("identity 'reader' on Kafka cluster 'local'");
    assertThat(events.log).contains("blocked orders CONFIGURATION");

    client.topic.set(TopicStatus.EXISTS);
    client.next().ack(0);

    result.get(10, TimeUnit.SECONDS);
    await().atMost(TIMEOUT).until(() -> events.log.contains("unblocked orders"));
  }

  @Test
  void topicIsCheckedOncePerProducerUntilTimeout() throws Exception {
    DlqWriter writer = ordersWriter();
    writer.send(request());
    FakeClient client = clients.get(0);
    client.next().ack(0);
    CompletableFuture<RecordMetadata> second = writer.send(request());
    client.next().fail(new TimeoutException("Topic not present in metadata after 10000 ms."));
    client.next().ack(1);

    second.get(10, TimeUnit.SECONDS);
    assertThat(client.describes.get()).as("first send, and again after the timeout").isEqualTo(2);
  }

  @Test
  void timeoutOfSendFailedByReplacementChecksTheTopicAgain() throws Exception {
    DlqWriter writer = ordersWriter();
    writer.send(request());
    FakeClient client = clients.get(0);
    client.next().ack(0);
    CompletableFuture<RecordMetadata> second = writer.send(request());
    client
        .next()
        .fail(
            new ProducerReplacedException(
                new SaslAuthenticationException("bad secret"),
                new TimeoutException("Topic not present in metadata after 10000 ms.")));
    client.next().ack(1);

    second.get(10, TimeUnit.SECONDS);
    assertThat(client.describes.get()).as("the send's own timeout counts").isEqualTo(2);
  }

  @Test
  void timeoutsAreFoundInsideReplacement() {
    TimeoutException timeout = new TimeoutException("t");
    NetworkException other = new NetworkException("n");
    assertThat(DlqFailure.isTimeout(new ExecutionException(timeout))).isTrue();
    assertThat(DlqFailure.isTimeout(new ProducerReplacedException(other, timeout))).isTrue();
    assertThat(DlqFailure.isTimeout(new ProducerReplacedException(timeout, other))).isTrue();
    assertThat(DlqFailure.isTimeout(new ProducerReplacedException(other, other))).isFalse();
    assertThat(DlqFailure.isTimeout(other)).isFalse();
  }

  @Test
  void failingTopicCheckIsRetried() throws Exception {
    DlqWriter writer = ordersWriter();
    FakeClient client = clients.get(0);
    client.describeFailure.set(new KafkaException("admin client broken"));

    CompletableFuture<RecordMetadata> result = writer.send(request());
    client.next().ack(6);

    assertThat(result.get(10, TimeUnit.SECONDS).offset()).isEqualTo(6);
    assertThat(events.failures).hasSize(1);
    assertThat(events.failures.get(0).cause()).hasMessage("admin client broken");
  }

  @Test
  void authorizationAndSizeErrorsAreConfigurationFailures() throws Exception {
    DlqWriter writer = ordersWriter();
    final CompletableFuture<RecordMetadata> result = writer.send(request());
    FakeClient client = clients.get(0);

    client.next().fail(new TopicAuthorizationException("not authorized"));
    client.next().fail(new RecordTooLargeException("too large"));
    client.next().ack(9);

    result.get(10, TimeUnit.SECONDS);
    assertThat(events.failures)
        .extracting(DlqFailure::kind)
        .containsExactly(DlqFailure.Kind.CONFIGURATION, DlqFailure.Kind.CONFIGURATION);
    assertThat(events.failures).extracting(DlqFailure::attempt).containsExactly(1, 2);
  }

  @Test
  void blockedIsReportedOnceForConcurrentFailures() throws Exception {
    DlqWriter writer = ordersWriter();
    CompletableFuture<RecordMetadata> a = writer.send(request());
    CompletableFuture<RecordMetadata> b = writer.send(request());
    FakeClient client = clients.get(0);

    client.next().fail(new NetworkException("x"));
    client.next().fail(new NetworkException("x"));
    client.next().ack(1);
    await().atMost(TIMEOUT).until(() -> a.isDone() || b.isDone());
    assertThat(writer.isBlocked()).as("one send still failing").isTrue();
    client.next().ack(2);

    CompletableFuture.allOf(a, b).get(10, TimeUnit.SECONDS);
    await().atMost(TIMEOUT).until(() -> !writer.isBlocked());
    assertThat(events.log.stream().filter(e -> e.startsWith("blocked"))).hasSize(1);
    assertThat(events.log.stream().filter(e -> e.startsWith("unblocked"))).hasSize(1);
  }

  @Test
  void closeAbandonsUnacknowledgedSendsAndRejectsNewOnes() {
    DlqWriter writer = ordersWriter();
    FakeClient client = clients.get(0);
    client.topic.set(TopicStatus.MISSING);
    CompletableFuture<RecordMetadata> waiting = writer.send(request());
    await().atMost(TIMEOUT).until(() -> client.describes.get() >= 1);

    producers.close();

    assertThatThrownBy(() -> waiting.get(10, TimeUnit.SECONDS))
        .isInstanceOf(ExecutionException.class)
        .hasCauseInstanceOf(DlqClosedException.class);
    assertThat(writer.send(request())).isCompletedExceptionally();
    assertThatThrownBy(() -> producers.writer("orders")).isInstanceOf(IllegalStateException.class);
    assertThat(client.closed).isTrue();
    assertThat(events.log).noneMatch(e -> e.startsWith("written"));
  }

  @Test
  void evictAbandonsTheWritersSendsAndKeepsTheOthers() throws Exception {
    producers(
        Map.of(
            "orders", pipeline("orders.v2", "reader", null),
            "payments", pipeline("payments", "reader", null)));
    DlqWriter orders = producers.writer("orders");
    DlqWriter payments = producers.writer("payments");
    FakeClient client = clients.get(0);
    final CompletableFuture<RecordMetadata> inFlight = orders.send(request());
    final PendingSend sent = client.next();
    final CompletableFuture<RecordMetadata> retrying = orders.send(request());
    client.next().fail(new NetworkException("broker gone"));
    await().atMost(TIMEOUT).until(() -> events.log.contains("blocked orders TRANSIENT"));
    final CompletableFuture<RecordMetadata> other = payments.send(request());

    producers.evict("orders");

    for (CompletableFuture<RecordMetadata> abandoned : List.of(inFlight, retrying)) {
      assertThatThrownBy(() -> abandoned.get(10, TimeUnit.SECONDS))
          .isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(DlqClosedException.class);
    }
    assertThat(orders.send(request())).isCompletedExceptionally();
    sent.ack(3); // a late acknowledgement of an abandoned send changes nothing
    Thread.sleep(100); // past several retry delays
    assertThat(events.log).noneMatch(e -> e.startsWith("written orders"));
    assertThat(events.log).noneMatch(e -> e.startsWith("unblocked"));
    assertThat(producers.writer("orders")).isNotSameAs(orders);
    assertThat(producers.writer("payments")).isSameAs(payments);
    assertThat(client.closed).as("the shared producer stays").isFalse();
    // The retrying send may have reached the producer before the eviction: acknowledge them all.
    await()
        .atMost(TIMEOUT)
        .until(() -> {
          PendingSend next = client.sends.poll();
          if (next != null) {
            next.ack(8);
          }
          return other.isDone();
        });
    assertThat(other.get(10, TimeUnit.SECONDS).offset()).isEqualTo(8);
    assertThat(events.log).noneMatch(e -> e.startsWith("written orders"));
    producers.evict("unknown"); // no-op
  }

  @Test
  void anAcknowledgementDuringCloseStillCompletes() throws Exception {
    DlqWriter writer = ordersWriter();
    CompletableFuture<RecordMetadata> result = writer.send(request());
    FakeClient client = clients.get(0);
    PendingSend sent = client.next();
    client.onClose = () -> sent.ack(4);

    producers.close();

    assertThat(result.get(10, TimeUnit.SECONDS).offset()).isEqualTo(4);
  }

  @Test
  void callersCannotCompleteTheSendThemselves() throws Exception {
    DlqWriter writer = ordersWriter();
    CompletableFuture<RecordMetadata> result = writer.send(request());

    result.complete(null);

    FakeClient client = clients.get(0);
    client.next().ack(1);
    await().atMost(TIMEOUT).until(() -> events.log.contains("written orders 1"));
    assertThat(writer.isBlocked()).isFalse();
    assertThat(result.get()).as("the caller's own value, not an acknowledgement").isNull();
  }

  @Test
  void oneProducerPerClusterAndIdentity() {
    Map<String, Pipeline> pipelines = new LinkedHashMap<>();
    pipelines.put("orders", pipeline("orders.v2", "reader", null));
    pipelines.put("payments", pipeline("payments.v1", "reader", "reader"));
    pipelines.put("audit", pipeline("audit.v1", "reader", "writer"));
    DlqProducers registry = producers(pipelines);

    DlqWriter orders = registry.writer("orders");
    registry.writer("payments");
    registry.writer("audit");

    assertThat(registry.writer("orders")).isSameAs(orders);
    assertThat(registry.size()).isEqualTo(2);
    assertThat(clients)
        .extracting(c -> c.settings.get("client.id"))
        .containsExactly("kafkafim-local-reader-dlq", "kafkafim-local-writer-dlq");
    assertThat(registry.writer("audit").topic()).isEqualTo("audit.v1.kafkafim-dlq");
  }

  @Test
  void unknownPipelineIsRejected() {
    DlqProducers registry = producers(Map.of());

    assertThatThrownBy(() -> registry.writer("nope"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("nope");
  }

  @Test
  void checkTopicDescribesWithTheDlqIdentity() {
    DlqProducers registry = producers(Map.of("audit", pipeline("audit.v1", "reader", "writer")));
    registry.writer("audit");
    clients.get(0).topic.set(TopicStatus.NOT_AUTHORIZED);

    assertThat(registry.checkTopic("audit")).isEqualTo(TopicStatus.NOT_AUTHORIZED);
    assertThat(clients.get(0).settings.get("client.id")).isEqualTo("kafkafim-local-writer-dlq");
  }

  @Test
  void throwingListenerDoesNotStopTheSend() throws Exception {
    producers =
        new DlqProducers(
            properties(Map.of("orders", pipeline("orders.v2", null, null))),
            new DlqListener() {
              @Override
              public void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {
                throw new IllegalStateException("listener bug");
              }
            },
            FAST,
            settings -> {
              FakeClient client = new FakeClient(settings);
              clients.add(client);
              return client;
            });
    CompletableFuture<RecordMetadata> result = producers.writer("orders").send(request());
    clients.get(0).next().ack(2);

    assertThat(result.get(10, TimeUnit.SECONDS).offset()).isEqualTo(2);
  }

  @Test
  void lifecyclePhaseIsBetweenSourcesAndWebServer() {
    DlqProducers registry = producers(Map.of());

    assertThat(registry.getPhase())
        .isLessThan(Integer.MAX_VALUE - 100)
        .isGreaterThan(Integer.MAX_VALUE - 1024);
    registry.start();
    assertThat(registry.isRunning()).isTrue();
    registry.stop();
    assertThat(registry.isRunning()).isFalse();
  }

  @Test
  void classification() {
    assertThat(DlqFailure.classify(new SaslAuthenticationException("x")))
        .isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(
            DlqFailure.classify(
                new ExecutionException(new TopicAuthorizationException("not authorized"))))
        .isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(DlqFailure.classify(new DlqTopicException(TopicStatus.MISSING, "m")))
        .isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(DlqFailure.classify(new DlqTopicException(TopicStatus.UNREACHABLE, "u")))
        .isEqualTo(DlqFailure.Kind.TRANSIENT);
    assertThat(DlqFailure.classify(new TimeoutException("t"))).isEqualTo(DlqFailure.Kind.TRANSIENT);
    assertThat(DlqFailure.classify(new IllegalStateException("?")))
        .isEqualTo(DlqFailure.Kind.TRANSIENT);
  }

  @Test
  void backoffDoublesWithJitterUpToTheCap() {
    DlqOptions options = DlqOptions.DEFAULTS;

    for (int i = 0; i < 100; i++) {
      assertThat(options.backoff(1).toMillis()).isBetween(75L, 125L);
      assertThat(options.backoff(3).toMillis()).isBetween(300L, 500L);
      assertThat(options.backoff(50).toMillis()).isBetween(22_500L, 37_500L);
      assertThat(options.backoff(Integer.MAX_VALUE).toMillis()).isBetween(22_500L, 37_500L);
    }
    assertThatThrownBy(() ->
            new DlqOptions(
                Duration.ofSeconds(2), Duration.ofSeconds(1), TIMEOUT, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /** Records listener calls in order. */
  private static final class Events implements DlqListener {
    final List<String> log = new CopyOnWriteArrayList<>();
    final List<DlqFailure> failures = new CopyOnWriteArrayList<>();

    @Override
    public void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {
      log.add("written " + pipeline + " " + metadata.offset());
    }

    @Override
    public void onSendFailed(String pipeline, DlqRequest request, DlqFailure failure) {
      failures.add(failure);
      log.add("failed " + pipeline + " " + failure.kind() + " " + failure.attempt());
    }

    @Override
    public void onBlocked(String pipeline, DlqFailure failure) {
      log.add("blocked " + pipeline + " " + failure.kind());
    }

    @Override
    public void onUnblocked(String pipeline) {
      log.add("unblocked " + pipeline);
    }
  }

  private record PendingSend(ProducerRecord<byte[], byte[]> record, Callback callback) {
    void ack(long offset) {
      callback.onCompletion(
          new RecordMetadata(new TopicPartition(record.topic(), 0), offset, 0, 0L, 1, 1), null);
    }

    void fail(Exception exception) {
      callback.onCompletion(null, exception);
    }
  }

  /** A producer whose acknowledgements the test hands out. */
  private static final class FakeClient implements DlqClient {
    final Map<String, Object> settings;
    final LinkedBlockingQueue<PendingSend> sends = new LinkedBlockingQueue<>();
    final AtomicReference<TopicStatus> topic = new AtomicReference<>(TopicStatus.EXISTS);
    final AtomicInteger describes = new AtomicInteger();
    final AtomicReference<RuntimeException> describeFailure = new AtomicReference<>();
    volatile Runnable onClose = () -> {};
    volatile boolean closed;

    FakeClient(Map<String, Object> settings) {
      this.settings = settings;
    }

    PendingSend next() throws InterruptedException {
      PendingSend send = sends.poll(10, TimeUnit.SECONDS);
      assertThat(send).as("a send reached the producer").isNotNull();
      return send;
    }

    @Override
    public void send(ProducerRecord<byte[], byte[]> record, Callback callback) {
      if (closed) {
        throw new IllegalStateException("closed");
      }
      sends.add(new PendingSend(record, callback));
    }

    @Override
    public TopicStatus describe(String name, Duration timeout) {
      describes.incrementAndGet();
      RuntimeException failure = describeFailure.getAndSet(null);
      if (failure != null) {
        throw failure;
      }
      return topic.get();
    }

    @Override
    public void close(Duration timeout) {
      onClose.run();
      closed = true;
    }
  }
}
