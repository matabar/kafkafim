package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.Callback;
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
    MockProducer<byte[], byte[]>[] holder = new MockProducer[1];
    KafkaDlqClient client =
        new KafkaDlqClient(
            Map.of("client.id", "c"),
            s -> {
              MockProducer<byte[], byte[]> producer =
                  new MockProducer<>(
                      false, null, new ByteArraySerializer(), new ByteArraySerializer()) {
                    @Override
                    public void close(Duration timeout) {
                      if (holder[0] == this) {
                        throw new KafkaException("close failed");
                      }
                      super.close(timeout);
                    }
                  };
              if (holder[0] == null) {
                holder[0] = producer;
              }
              created.add(producer);
              return producer;
            });
    holder[0].sendException = fatalWrapper();

    assertThatThrownBy(() -> client.send(record(), (m, e) -> {}))
        .isInstanceOf(KafkaException.class);

    assertThat(client.producer()).isNotSameAs(holder[0]);
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
