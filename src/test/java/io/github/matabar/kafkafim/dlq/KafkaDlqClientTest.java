package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** Replacement of a producer that kafka-clients put into a fatal error state. */
class KafkaDlqClientTest {

  private final List<MockProducer<byte[], byte[]>> created = new CopyOnWriteArrayList<>();
  private final AtomicInteger creationFailures = new AtomicInteger();

  private KafkaDlqClient client() {
    Map<String, Object> settings =
        Map.of("bootstrap.servers", "localhost:9092", "client.id", "kafkafim-local-dlq");
    return new KafkaDlqClient(
        settings,
        s -> {
          if (creationFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
            throw new KafkaException(
                "Failed to construct kafka producer",
                new ConfigException("No resolvable bootstrap urls"));
          }
          MockProducer<byte[], byte[]> producer =
              new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer());
          created.add(producer);
          return producer;
        });
  }

  private static ProducerRecord<byte[], byte[]> record() {
    return new ProducerRecord<>("dlq", new byte[] {1}, new byte[] {2});
  }

  /** What kafka-clients throws once the idempotent producer is in a fatal state. */
  private static KafkaException fatalWrapper() {
    return new KafkaException(
        "Cannot execute transactional method because we are in an error state",
        new ClusterAuthorizationException("Cluster authorization failed."));
  }

  @Test
  void fatalSendExceptionReplacesTheProducer() {
    KafkaDlqClient client = client();
    MockProducer<byte[], byte[]> first = created.get(0);
    first.sendException = fatalWrapper();

    assertThatThrownBy(() -> client.send(record(), (m, e) -> {})).isSameAs(first.sendException);

    assertThat(created).hasSize(2);
    assertThat(first.closed()).isTrue();
    assertThat(client.producer()).isSameAs(created.get(1));
    client.send(record(), (m, e) -> {});
    assertThat(created.get(1).history()).hasSize(1);
    assertThat(DlqFailure.classify(fatalWrapper())).isEqualTo(DlqFailure.Kind.CONFIGURATION);
  }

  @Test
  void fatalCallbackFailureReplacesTheProducerBeforeTheCallerHearsOfIt() {
    KafkaDlqClient client = client();
    MockProducer<byte[], byte[]> first = created.get(0);
    AtomicReference<Exception> heard = new AtomicReference<>();
    AtomicReference<Integer> producersWhenHeard = new AtomicReference<>();
    client.send(
        record(),
        (m, e) -> {
          heard.set(e);
          producersWhenHeard.set(created.size());
        });

    first.errorNext(new SaslAuthenticationException("bad password"));

    assertThat(heard.get()).isInstanceOf(SaslAuthenticationException.class);
    assertThat(producersWhenHeard.get()).isEqualTo(2);
    assertThat(first.closed()).isTrue();
  }

  @Test
  void sendThatThrowsAfterAppendingIsReportedOnlyByTheThrow() {
    // KafkaProducer.send appends the record, then throws when its transaction manager is in an
    // error state; closing the producer for the replacement aborts that appended batch inside the
    // close (close(ZERO) off the I/O thread joins it), before the throw reaches the caller.
    AtomicReference<MockProducer<byte[], byte[]>> first = new AtomicReference<>();
    AtomicReference<Boolean> abortedInClose = new AtomicReference<>();
    KafkaDlqClient client =
        new KafkaDlqClient(
            Map.of("client.id", "c"),
            s -> {
              MockProducer<byte[], byte[]> producer =
                  new MockProducer<>(
                      false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                    @Override
                    public synchronized Future<RecordMetadata> send(
                        ProducerRecord<byte[], byte[]> record, Callback callback) {
                      Future<RecordMetadata> appended = super.send(record, callback);
                      if (first.get() == this) {
                        throw fatalWrapper();
                      }
                      return appended;
                    }

                    @Override
                    public void close(Duration timeout) {
                      if (first.get() == this) {
                        abortedInClose.set(
                            errorNext(new KafkaException("Producer is closed forcefully.")));
                      }
                      super.close(timeout);
                    }
                  };
              first.compareAndSet(null, producer);
              created.add(producer);
              return producer;
            });
    List<Exception> heard = new CopyOnWriteArrayList<>();

    assertThatThrownBy(() -> client.send(record(), (m, e) -> heard.add(e)))
        .hasCauseInstanceOf(ClusterAuthorizationException.class);

    assertThat(abortedInClose.get()).as("the close aborted the appended batch").isTrue();
    assertThat(heard).as("the aborted batch is not a second report").isEmpty();
    assertThat(first.get().closed()).isTrue();
    assertThat(client.producer()).isSameAs(created.get(1));
  }

  @Test
  void sendAbortedByTheReplacementIsReportedByTheFailureThatTriggeredIt() {
    KafkaDlqClient client = client();
    MockProducer<byte[], byte[]> first = created.get(0);
    AtomicReference<Exception> trigger = new AtomicReference<>();
    AtomicReference<Exception> bystander = new AtomicReference<>();
    client.send(record(), (m, e) -> trigger.set(e));
    client.send(record(), (m, e) -> bystander.set(e));
    KafkaException closedForcefully = new KafkaException("Producer is closed forcefully.");

    first.errorNext(fatalWrapper());
    first.errorNext(closedForcefully);

    assertThat(DlqFailure.classify(trigger.get())).isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(bystander.get())
        .isInstanceOf(ProducerReplacedException.class)
        .hasRootCauseInstanceOf(ClusterAuthorizationException.class)
        .hasMessageContaining("Producer is closed forcefully.");
    assertThat(bystander.get().getSuppressed()).containsExactly(closedForcefully);
    assertThat(DlqFailure.classify(bystander.get())).isEqualTo(DlqFailure.Kind.CONFIGURATION);
    assertThat(created).as("replaced once").hasSize(2);
  }

  @Test
  void failedReplacementIsRetriedOnTheNextSend() {
    KafkaDlqClient client = client();
    MockProducer<byte[], byte[]> first = created.get(0);
    first.sendException = fatalWrapper();
    creationFailures.set(2);

    assertThatThrownBy(() -> client.send(record(), (m, e) -> {})).isSameAs(first.sendException);
    assertThat(created).as("replacement failed").hasSize(1);
    assertThatThrownBy(() -> client.send(record(), (m, e) -> {}))
        .isInstanceOf(KafkaException.class)
        .hasCauseInstanceOf(ConfigException.class)
        .satisfies(
            e -> assertThat(DlqFailure.classify(e)).isEqualTo(DlqFailure.Kind.CONFIGURATION));

    client.send(record(), (m, e) -> {});

    assertThat(created).hasSize(2);
    assertThat(created.get(1).history()).hasSize(1);
    assertThat(first.closed()).isTrue();
  }

  @Test
  void failingCloseOfTheOldProducerStillReplacesIt() {
    AtomicReference<MockProducer<byte[], byte[]>> first = new AtomicReference<>();
    KafkaDlqClient client =
        new KafkaDlqClient(
            Map.of("client.id", "c"),
            s -> {
              MockProducer<byte[], byte[]> producer =
                  new MockProducer<>(
                      false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                    @Override
                    public void close(Duration timeout) {
                      if (first.get() == this) {
                        throw new KafkaException("close failed");
                      }
                      super.close(timeout);
                    }
                  };
              first.compareAndSet(null, producer);
              created.add(producer);
              return producer;
            });
    first.get().sendException = fatalWrapper();

    assertThatThrownBy(() -> client.send(record(), (m, e) -> {}))
        .isInstanceOf(KafkaException.class);

    assertThat(client.producer()).isNotSameAs(first.get());
    assertThat(created).hasSize(2);
  }

  @Test
  void transientFailureKeepsTheProducer() {
    KafkaDlqClient client = client();
    AtomicReference<Exception> heard = new AtomicReference<>();
    client.send(record(), (m, e) -> heard.set(e));
    NetworkException gone = new NetworkException("broker gone");

    created.get(0).errorNext(gone);

    assertThat(heard.get()).as("reported as is").isSameAs(gone);
    assertThat(created).hasSize(1);
    assertThat(created.get(0).closed()).isFalse();
  }

  @Test
  void noReplacementAfterClose() {
    KafkaDlqClient client = client();
    MockProducer<byte[], byte[]> first = created.get(0);
    client.close(Duration.ZERO);
    first.sendException = fatalWrapper();

    try {
      client.send(record(), (m, e) -> {});
    } catch (RuntimeException expected) {
      // a closed producer may throw or fail the send; either way nothing is recreated
    }

    assertThat(created).hasSize(1);
  }

  /**
   * A client whose producer's close blocks until {@code release} opens, as against a silent broker.
   */
  private static KafkaDlqClient clientWithHangingClose(
      CountDownLatch release, AtomicReference<Duration> closedWith, Duration grace) {
    return new KafkaDlqClient(
        Map.of("bootstrap.servers", "localhost:9092", "client.id", "kafkafim-local-dlq"),
        s ->
            new MockProducer<>(false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
              @Override
              public void close(Duration timeout) {
                closedWith.set(timeout);
                try {
                  release.await();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                }
                super.close(timeout);
              }
            },
        grace);
  }

  @Test
  void closeReturnsAfterItsTimeoutAndGraceWhenTheProducerDoesNotFinishClosing() {
    CountDownLatch release = new CountDownLatch(1);
    AtomicReference<Duration> closedWith = new AtomicReference<>();
    KafkaDlqClient client = clientWithHangingClose(release, closedWith, Duration.ofMillis(100));
    try {
      long start = System.nanoTime();
      client.close(Duration.ofMillis(200));
      Duration took = Duration.ofNanos(System.nanoTime() - start);

      assertThat(took)
          .isGreaterThanOrEqualTo(Duration.ofMillis(300))
          .isLessThan(Duration.ofSeconds(5));
      assertThat(closedWith.get())
          .as("the producer still gets the whole timeout")
          .isEqualTo(Duration.ofMillis(200));
      assertThat(((MockProducer<byte[], byte[]>) client.producer()).closed()).isFalse();
    } finally {
      release.countDown();
    }
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> ((MockProducer<byte[], byte[]>) client.producer()).closed());
  }

  @Test
  void closeWaitsForProducerThatFinishesWithinTheGrace() {
    CountDownLatch release = new CountDownLatch(1);
    KafkaDlqClient client =
        clientWithHangingClose(release, new AtomicReference<>(), Duration.ofSeconds(10));
    Thread.ofVirtual()
        .start(() -> {
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          release.countDown();
        });

    client.close(Duration.ZERO);

    assertThat(((MockProducer<byte[], byte[]>) client.producer()).closed()).isTrue();
  }

  @Test
  void failingProducerCloseReachesTheCaller() {
    KafkaException failure = new KafkaException("close failed");
    KafkaDlqClient client =
        new KafkaDlqClient(
            Map.of("bootstrap.servers", "localhost:9092", "client.id", "kafkafim-local-dlq"),
            s ->
                new MockProducer<>(
                    false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                  @Override
                  public void close(Duration timeout) {
                    throw failure;
                  }
                });

    assertThatThrownBy(() -> client.close(Duration.ofSeconds(1))).isSameAs(failure);
  }

  @Test
  void closeIsBoundedAgainstBrokerThatAcceptsAndNeverAnswers() throws Exception {
    try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      List<Socket> accepted = new CopyOnWriteArrayList<>();
      Thread.ofVirtual()
          .start(() -> {
            while (!silent.isClosed()) {
              try {
                accepted.add(silent.accept());
              } catch (IOException e) {
                return;
              }
            }
          });
      Map<String, Object> settings = new HashMap<>();
      settings.put("bootstrap.servers", "127.0.0.1:" + silent.getLocalPort());
      settings.put("client.id", "kafkafim-silent-dlq");
      settings.put("enable.idempotence", true);
      settings.put("acks", "all");
      settings.put("key.serializer", ByteArraySerializer.class);
      settings.put("value.serializer", ByteArraySerializer.class);
      KafkaDlqClient client =
          new KafkaDlqClient(settings, KafkaProducer::new, Duration.ofMillis(500));
      try {
        assertThat(client.describe("dlq", Duration.ofMillis(300)))
            .isEqualTo(TopicStatus.UNREACHABLE);
        await().atMost(Duration.ofSeconds(10)).until(() -> !accepted.isEmpty());

        long start = System.nanoTime();
        client.close(Duration.ofMillis(200));
        Duration took = Duration.ofNanos(System.nanoTime() - start);

        assertThat(took).as("not request.timeout.ms (30s)").isLessThan(Duration.ofSeconds(5));
      } finally {
        for (Socket socket : accepted) {
          socket.close();
        }
      }
    }
  }

  @Test
  void fatalCausesAreFoundAnywhereInTheChain() {
    assertThat(DlqFailure.isFatalForProducer(fatalWrapper())).isTrue();
    assertThat(DlqFailure.isFatalForProducer(new KafkaException("x", new NetworkException("n"))))
        .isFalse();
    assertThat(
            DlqFailure.classify(
                new KafkaException(
                    "Failed to create new KafkaAdminClient",
                    new org.apache.kafka.common.config.ConfigException("bad"))))
        .isEqualTo(DlqFailure.Kind.CONFIGURATION);
  }
}
