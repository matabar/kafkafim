package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;
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

  @Test
  void refusesInlineSecretsInTheKafkafimProfileOnKubernetes() {
    MockEnvironment environment = environment("kubernetes", "kafkafim");
    environment.setProperty("kafkafim.allow-inline-secrets", "true");

    assertThatIllegalStateException()
        .isThrownBy(() -> LocalProfileGuard.check(environment))
        .withMessageContaining("kafkafim.allow-inline-secrets=true is set on Kubernetes")
        .withMessageContaining("mounted Secret files")
        .withMessageContaining("configtree");
  }

  @Test
  void refusesInlineSecretsSetThroughTheEnvironmentVariableName() {
    MockEnvironment environment = environment("kubernetes", "kafkafim");
    environment
        .getPropertySources()
        .addFirst(
            new SystemEnvironmentPropertySource(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                Map.of("KAFKAFIM_ALLOWINLINESECRETS", "true")));

    assertThatIllegalStateException()
        .isThrownBy(() -> LocalProfileGuard.check(environment))
        .withMessageContaining("allow-inline-secrets");
  }

  @Test
  void allowsInlineSecretsSetToFalseOnKubernetes() {
    MockEnvironment environment = environment("kubernetes", "kafkafim");
    environment.setProperty("kafkafim.allow-inline-secrets", "false");

    assertThatCode(() -> LocalProfileGuard.check(environment)).doesNotThrowAnyException();
  }

  @Test
  void allowsInlineSecretsOffKubernetes() {
    MockEnvironment environment = environment("none", "kafkafim");
    environment.setProperty("kafkafim.allow-inline-secrets", "true");

    assertThatCode(() -> LocalProfileGuard.check(environment)).doesNotThrowAnyException();
  }
}
