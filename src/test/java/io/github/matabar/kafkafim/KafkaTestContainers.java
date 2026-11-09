package io.github.matabar.kafkafim;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** The single-node Kafka broker every integration test runs against. */
public final class KafkaTestContainers {

  /** The broker image; its GraalVM native launcher starts in about a second. */
  public static final DockerImageName IMAGE = DockerImageName.parse("apache/kafka-native:4.1.0");

  /**
   * The JVM image of the same Kafka version, for tests that need a SASL listener: the native broker
   * fails every SASL handshake ({@code UnsupportedOperationException: Unable to find suitable
   * Subject#doAs or Subject#callAs implementation}). It starts in a few seconds.
   */
  public static final DockerImageName JVM_IMAGE = DockerImageName.parse("apache/kafka:4.1.0");

  /**
   * How often Testcontainers creates a fresh container when one fails to start. The native launcher
   * occasionally segfaults in {@code getpwuid} before Kafka starts (CI run 42473368699, once in
   * about 100 runs); a fresh container starts normally.
   */
  static final int STARTUP_ATTEMPTS = 3;

  private KafkaTestContainers() {}

  /** A new, not yet started Kafka container that retries a failed startup. */
  public static KafkaContainer kafka() {
    return new KafkaContainer(IMAGE).withStartupAttempts(STARTUP_ATTEMPTS);
  }

  /** A new, not yet started Kafka container on {@link #JVM_IMAGE}, for SASL and ACL tests. */
  public static KafkaContainer jvmKafka() {
    return new KafkaContainer(JVM_IMAGE).withStartupAttempts(STARTUP_ATTEMPTS);
  }
}
