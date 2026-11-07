package io.github.matabar.kafkafim.dlq;

import java.time.Duration;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The Kafka side of one DLQ producer per {@code (cluster, identity)}: a thread-safe producer plus
 * the topic check. {@link KafkaDlqClient} in production; a fake in unit tests.
 */
interface DlqClient {

  /**
   * Sends one record; may block up to {@code max.block.ms} for metadata or buffer space.
   *
   * @param record the record
   * @param callback called once with the metadata or the failure, on the producer's I/O thread
   * @throws RuntimeException if the send fails before it was handed to the producer
   */
  void send(ProducerRecord<byte[], byte[]> record, Callback callback);

  /**
   * Describes a topic; blocks up to {@code timeout}.
   *
   * @param topic the topic
   * @param timeout how long to wait
   * @return what the broker said
   */
  TopicStatus describe(String topic, Duration timeout);

  /**
   * Flushes outstanding sends for at most {@code timeout} and releases the clients.
   *
   * @param timeout upper bound
   */
  void close(Duration timeout);
}
