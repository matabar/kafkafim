package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.KafkafimApplication;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Starts the real application with development settings (the {@code local} profile, or inline
 * secrets allowed) on (simulated) Kubernetes and expects it to refuse (DESIGN §5.4, §5.5). Fails
 * before any client or container is created, so no Docker is needed.
 */
class LocalProfileGuardIntegrationTest {

  @Test
  void refusesToStartWithTheDefaultProfileOnKubernetes() {
    // No profile set: application.yaml's spring.profiles.default=local applies, as in a pod that
    // lost SPRING_PROFILES_ACTIVE.
    SpringApplicationBuilder application =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .properties(
                "spring.main.cloud-platform=kubernetes",
                "spring.docker.compose.enabled=false",
                "server.port=0");

    assertThatThrownBy(application::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("SPRING_PROFILES_ACTIVE=kafkafim");
  }

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

  @Test
  void refusesToStartWithInlineSecretsAllowedOnKubernetes() {
    // The local profile is off, so only the flag itself can trip the guard.
    SpringApplicationBuilder application =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .profiles("kafkafim")
            .properties(
                "spring.main.cloud-platform=kubernetes",
                "kafkafim.allow-inline-secrets=true",
                "spring.docker.compose.enabled=false",
                "server.port=0");

    assertThatThrownBy(application::run)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("kafkafim.allow-inline-secrets=true is set on Kubernetes")
        .hasMessageContaining("configtree");
  }
}
