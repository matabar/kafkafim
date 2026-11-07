package io.github.matabar.kafkafim;

import org.springframework.boot.SpringApplication;

/** Runs the application locally against Testcontainers-managed Kafka, Elasticsearch and LGTM. */
public class TestKafkafimApplication {

  /** Starts the application with the Testcontainers configuration applied. */
  public static void main(String[] args) {
    SpringApplication.from(KafkafimApplication::main)
        .with(TestcontainersConfiguration.class)
        .run(args);
  }
}
