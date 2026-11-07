package io.github.matabar.kafkafim;

import java.util.Arrays;
import java.util.stream.Stream;
import org.springframework.boot.SpringApplication;

/** Runs the application locally against the Testcontainers-managed infrastructure. */
public class TestKafkafimApplication {

  /**
   * Starts the application with the Testcontainers configuration applied. Docker Compose support is
   * disabled so compose.yaml (meant for {@code spring-boot:run}) does not start a second stack.
   */
  public static void main(String[] args) {
    String[] arguments =
        Stream.concat(Arrays.stream(args), Stream.of("--spring.docker.compose.enabled=false"))
            .toArray(String[]::new);
    SpringApplication.from(KafkafimApplication::main)
        .with(TestcontainersConfiguration.class)
        .run(arguments);
  }
}
