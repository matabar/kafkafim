package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

class LocalProfileGuardTest {

  private static MockEnvironment environment(String platform, String... activeProfiles) {
    MockEnvironment environment = new MockEnvironment();
    environment.setDefaultProfiles("local");
    environment.setActiveProfiles(activeProfiles);
    if (platform != null) {
      environment.setProperty("spring.main.cloud-platform", platform);
    }
    return environment;
  }

  @Test
  void refusesTheDefaultLocalProfileOnKubernetes() {
    assertThatIllegalStateException()
        .isThrownBy(() -> LocalProfileGuard.check(environment("kubernetes")))
        .withMessageContaining("SPRING_PROFILES_ACTIVE=kafkafim");
  }

  @Test
  void refusesAnExplicitLocalProfileOnKubernetes() {
    assertThatIllegalStateException()
        .isThrownBy(() -> LocalProfileGuard.check(environment("kubernetes", "kafkafim", "local")));
  }

  @Test
  void allowsTheKafkafimProfileOnKubernetes() {
    assertThatCode(() -> LocalProfileGuard.check(environment("kubernetes", "kafkafim")))
        .doesNotThrowAnyException();
  }

  @Test
  void allowsTheLocalProfileOffKubernetes() {
    assertThatCode(() -> LocalProfileGuard.check(environment("none"))).doesNotThrowAnyException();
    assertThatCode(() -> LocalProfileGuard.check(environment(null))).doesNotThrowAnyException();
  }
}
