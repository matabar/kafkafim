package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;

/** Re-reading config data from disk with startup's precedence (DESIGN §5.6). */
class FreshEnvironmentTest {

  @TempDir Path dir;

  @Configuration(proxyBeanMethods = false)
  static class Empty {}

  /** A real Boot environment over a config directory, a configtree and default properties. */
  private ConfigurableApplicationContext start(Map<String, Object> defaults) {
    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("spring.config.additional-location", "file:" + dir.resolve("config") + "/");
    properties.put("spring.config.import", "optional:configtree:" + dir.resolve("secrets") + "/");
    properties.putAll(defaults);
    return new SpringApplicationBuilder(Empty.class)
        .profiles("fresh-test")
        .properties(properties)
        // application.yaml asks for a servlet server; a command-line argument beats it.
        .run("--spring.main.web-application-type=none", "--probe.cli=c");
  }

  @Test
  void filesAndSecretsAreReadAgainWithTheSamePrecedence() throws Exception {
    Path config = Files.createDirectories(dir.resolve("config"));
    Path secrets = Files.createDirectories(dir.resolve("secrets"));
    Files.writeString(
        config.resolve("application-fresh-test.yaml"),
        "probe:\n  file: one\n  overridden: from-file\n  defaulted: from-file\n");
    Files.writeString(secrets.resolve("probe.secret"), "s1");
    System.setProperty("probe.overridden", "from-system-property");
    try (ConfigurableApplicationContext app =
        start(Map.of("probe.defaulted", "from-defaults", "probe.only-default", "d"))) {
      ConfigurableEnvironment running = app.getEnvironment();
      assertThat(running.getProperty("probe.file")).isEqualTo("one");

      Files.writeString(
          config.resolve("application-fresh-test.yaml"),
          "probe:\n  file: two\n  overridden: from-file\n  defaulted: from-file\n");
      Files.writeString(secrets.resolve("probe.secret"), "s2");
      ConfigurableEnvironment fresh = FreshEnvironment.load(running);

      assertThat(fresh.getProperty("probe.file")).isEqualTo("two");
      assertThat(fresh.getProperty("probe.secret")).isEqualTo("s2");
      // System properties still beat files; files still beat SpringApplication defaults.
      assertThat(fresh.getProperty("probe.overridden")).isEqualTo("from-system-property");
      assertThat(fresh.getProperty("probe.defaulted")).isEqualTo("from-file");
      assertThat(fresh.getProperty("probe.only-default")).isEqualTo("d");
      assertThat(fresh.getProperty("probe.cli")).isEqualTo("c");
      assertThat(fresh.getActiveProfiles()).containsExactly("fresh-test");
      // The running environment is untouched.
      assertThat(running.getProperty("probe.file")).isEqualTo("one");
      // Each config source appears once: the running one's file sources were not copied over.
      List<String> names =
          fresh.getPropertySources().stream().map(PropertySource::getName).toList();
      assertThat(names.stream().filter(n -> n.contains("application-fresh-test.yaml"))).hasSize(1);
    } finally {
      System.clearProperty("probe.overridden");
    }
  }

  @Test
  void anUnparsableFileFails() throws Exception {
    Path config = Files.createDirectories(dir.resolve("config"));
    Files.writeString(config.resolve("application-fresh-test.yaml"), "probe: ok\n");
    try (ConfigurableApplicationContext app = start(Map.of())) {
      Files.writeString(config.resolve("application-fresh-test.yaml"), "probe: [unclosed\n");
      assertThatThrownBy(() -> FreshEnvironment.load(app.getEnvironment()))
          .isInstanceOf(RuntimeException.class);
    }
  }
}
