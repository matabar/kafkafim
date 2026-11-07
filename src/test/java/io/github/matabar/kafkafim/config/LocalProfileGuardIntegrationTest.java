package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.KafkafimApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Starts the real application with the {@code local} profile on (simulated) Kubernetes and expects
 * it to refuse (DESIGN §5.4). Fails before any client or container is created, so no Docker is
 * needed.
 */
class LocalProfileGuardIntegrationTest {

  @Test
  void refusesToStartWithTheLocalProfileOnKubernetes() {
    SpringApplicationBuilder application =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .profiles("local")
            .properties(
                "spring.main.cloud-platform=kubernetes",
                "spring.docker.compose.enabled=false",
                "server.port=0");

    assertThatThrownBy(application::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("'local' profile")
        .hasMessageContaining("SPRING_PROFILES_ACTIVE=kafkafim");
  }
}
