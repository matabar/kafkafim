package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;

/**
 * Binds YAML through the real startup path ({@code @EnableConfigurationProperties} on {@link
 * KafkafimConfiguration}, Bean Validation plus {@link KafkafimPropertiesValidator}).
 */
final class ConfigTestSupport {

  /** One plaintext Kafka cluster {@code k}. */
  static final String KAFKA_CLUSTERS =
      """
      k:
        bootstrap-servers: [k1:9092]
      """;

  /** One plain-http Elasticsearch cluster {@code es}. */
  static final String ES_CLUSTERS =
      """
      es:
        uris: [http://es:9200]
      """;

  /** The routes of {@link #MINIMAL}. */
  static final String DEFAULT_ROUTE =
      """
      routes:
        - { default: true, cluster: es, index: idx }
      """;

  /** A minimal valid tree: Kafka cluster {@code k}, ES cluster {@code es}, pipeline {@code p}. */
  static final String MINIMAL = pipeline(DEFAULT_ROUTE);

  /** A tree with the given cluster maps and pipeline map, each written without base indentation. */
  static String tree(String kafkaClusters, String esClusters, String pipelines) {
    return "kafkafim:\n"
        + "  kafka:\n    clusters:\n"
        + kafkaClusters.indent(6)
        + "  elasticsearch:\n    clusters:\n"
        + esClusters.indent(6)
        + "  pipelines:\n"
        + pipelines.indent(4);
  }

  /**
   * {@link #KAFKA_CLUSTERS}, {@link #ES_CLUSTERS} and pipeline {@code p} reading topic {@code t}.
   */
  static String pipeline(String body) {
    return pipeline(KAFKA_CLUSTERS, ES_CLUSTERS, "source: { cluster: k, topics: [t] }\n" + body);
  }

  /** Pipeline {@code p} with the given body over the given clusters. */
  static String pipeline(String kafkaClusters, String esClusters, String body) {
    return tree(kafkaClusters, esClusters, "p:\n" + body.indent(2));
  }

  private ConfigTestSupport() {}

  static ApplicationContextRunner runner(String yaml, Map<String, Object> extraSources) {
    return new ApplicationContextRunner()
        .withInitializer(context -> {
          var sources = context.getEnvironment().getPropertySources();
          if (!extraSources.isEmpty()) {
            sources.addLast(new ConfigTreePropertySource("configtree", configTree(extraSources)));
          }
          load(yaml).forEach(sources::addLast);
        })
        .withUserConfiguration(KafkafimConfiguration.class);
  }

  /** Writes {@code secrets} as files of a temporary configtree, as a mounted Secret would be. */
  static Path configTree(Map<String, Object> secrets) {
    try {
      Path tree = Files.createTempDirectory("kafkafim-configtree");
      for (Map.Entry<String, Object> secret : secrets.entrySet()) {
        Files.writeString(tree.resolve(secret.getKey()), String.valueOf(secret.getValue()));
      }
      return tree;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  static List<PropertySource<?>> load(String yaml) {
    try {
      return new YamlPropertySourceLoader()
          .load(
              "test.yaml",
              new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8), "test.yaml"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Binds {@code yaml}, asserting it is valid. */
  static KafkafimProperties bind(String yaml, String... overrides) {
    return bind(yaml, Map.of(), overrides);
  }

  static KafkafimProperties bind(String yaml, Map<String, Object> secrets, String... overrides) {
    KafkafimProperties[] result = new KafkafimProperties[1];
    runner(yaml, secrets)
        .withPropertyValues(split(overrides))
        .run(context -> {
          assertThat(context).hasNotFailed();
          result[0] = context.getBean(KafkafimProperties.class);
        });
    return result[0];
  }

  /** Binds {@code yaml}, asserting it fails validation, and returns every reported violation. */
  static List<String> errors(String yaml, String... overrides) {
    return errors(yaml, Map.of(), overrides);
  }

  static List<String> errors(String yaml, Map<String, Object> secrets, String... overrides) {
    List<String> errors = new ArrayList<>();
    runner(yaml, secrets)
        .withPropertyValues(split(overrides))
        .run(context -> {
          assertThat(context).hasFailed();
          errors.addAll(messages(context));
        });
    return errors;
  }

  /** Binds {@code yaml}, asserting startup fails, and returns the failure. */
  static Throwable runnerFailure(String yaml, String... overrides) {
    Throwable[] failure = new Throwable[1];
    runner(yaml, Map.of())
        .withPropertyValues(split(overrides))
        .run(context -> {
          assertThat(context).hasFailed();
          failure[0] = context.getStartupFailure();
        });
    return failure[0];
  }

  /** Lets one override argument carry several properties, separated by newlines. */
  private static String[] split(String... overrides) {
    return Arrays.stream(overrides)
        .flatMap(o -> Arrays.stream(o.split("\n")))
        .toArray(String[]::new);
  }

  private static List<String> messages(AssertableApplicationContext context) {
    Throwable failure = context.getStartupFailure();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      if (t instanceof BindValidationException validation) {
        return validation.getValidationErrors().getAllErrors().stream()
            .map(ConfigTestSupport::describe)
            .toList();
      }
    }
    throw new AssertionError("expected a BindValidationException", failure);
  }

  private static String describe(ObjectError error) {
    if (error instanceof FieldError field) {
      return field.getField() + ": " + field.getDefaultMessage();
    }
    return error.getDefaultMessage();
  }

  /**
   * Whether a record component holds a secret, or a map of passthrough keys that may (DESIGN §5.5).
   * The redaction and Bean Validation guards share this rule. It matches the Java component name,
   * so a secret component must be named after what it holds (not {@code @Name("password") pw}), and
   * a new kind of secret (a token, say) must be added here.
   */
  static boolean holdsSecret(RecordComponent component) {
    String name = component.getName().toLowerCase(Locale.ROOT);
    return name.contains("password")
        || name.contains("secret")
        || name.contains("apikey")
        || name.equals("properties");
  }

  /**
   * Every record of the configuration model reachable from {@code root}, through record components
   * and generic type arguments. Arrays, wildcards, type variables and non-record component types
   * are not followed; the model has none that lead to a record.
   */
  static Set<Class<?>> recordsReachableFrom(Class<?> root) {
    Set<Class<?>> seen = new HashSet<>();
    List<Type> pending = new ArrayList<>(List.of(root));
    while (!pending.isEmpty()) {
      Type type = pending.removeLast();
      if (type instanceof ParameterizedType parameterized) {
        pending.addAll(List.of(parameterized.getActualTypeArguments()));
      } else if (type instanceof Class<?> cls && cls.isRecord() && seen.add(cls)) {
        for (RecordComponent component : cls.getRecordComponents()) {
          pending.add(component.getGenericType());
        }
      }
    }
    assertThat(seen).contains(KafkaCluster.Sasl.class, ElasticsearchCluster.Keystore.class);
    return seen;
  }
}
