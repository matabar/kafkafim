package io.github.matabar.kafkafim;

import io.github.matabar.kafkafim.config.ValidateProfile;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

/** Entry point of the kafkafim worker, which ships JSON records from Kafka to Elasticsearch. */
@SpringBootApplication
public class KafkafimApplication {

  /**
   * Starts the Spring Boot application. Under the {@code validate} profile nothing keeps running: a
   * valid configuration exits 0 once its summary is logged, an invalid one fails startup, which
   * exits 1 (DESIGN §5.6).
   */
  public static void main(String[] args) {
    ConfigurableApplicationContext context = SpringApplication.run(KafkafimApplication.class, args);
    if (ValidateProfile.isActive(context.getEnvironment())) {
      System.exit(SpringApplication.exit(context));
    }
  }
}
