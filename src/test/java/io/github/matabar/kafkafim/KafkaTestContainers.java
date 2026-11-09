package io.github.matabar.kafkafim;

import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** The single-node Kafka broker every integration test runs against. */
public final class KafkaTestContainers {

  /** The broker image; its GraalVM native launcher starts in about a second. */
  public static final DockerImageName IMAGE = DockerImageName.parse("apache/kafka-native:4.1.0");

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
}
