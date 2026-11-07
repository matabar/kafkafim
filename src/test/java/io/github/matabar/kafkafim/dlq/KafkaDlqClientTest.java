package io.github.matabar.kafkafim.dlq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.NetworkException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

/** Replacement of a producer that kafka-clients put into a fatal error state. */
class KafkaDlqClientTest {

  private final List<MockProducer<byte[], byte[]>> created = new CopyOnWriteArrayList<>();

  private KafkaDlqClient client() {
    Map<String, Object> settings =
        Map.of("bootstrap.servers", "localhost:9092", "client.id", "kafkafim-local-dlq");
    return new KafkaDlqClient(
        settings,
        s -> {
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
  void transientFailureKeepsTheProducer() {
    KafkaDlqClient client = client();
    client.send(record(), (m, e) -> {});

    created.get(0).errorNext(new NetworkException("broker gone"));

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
