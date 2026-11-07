package io.github.matabar.kafkafim;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Entry point of the kafkafim worker, which ships JSON records from Kafka to Elasticsearch. */
@SpringBootApplication
public class KafkafimApplication {

  /** Starts the Spring Boot application. */
  public static void main(String[] args) {
    SpringApplication.run(KafkafimApplication.class, args);
  }
}
