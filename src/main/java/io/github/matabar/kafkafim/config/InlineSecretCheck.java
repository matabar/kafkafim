package io.github.matabar.kafkafim.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.env.ConfigTreePropertySource;

/**
 * Refuses literal secret values (DESIGN §5.5). A secret must reach the tree either as a single
 * {@code ${...}} placeholder or straight from a mounted {@code configtree} file; anything else (a
 * literal in YAML, an environment variable, a system property, a placeholder with a literal
 * default) is reported.
 *
 * <p>Detection reads the <em>unresolved</em> value: placeholders are resolved by the {@code Binder}
 * while binding, not by the property sources, so the first {@link ConfigurationPropertySource} that
 * holds a secret's name (the same precedence the binder uses) still returns the raw text with its
 * {@code ${...}} intact, plus its origin. The check is model-driven: it only looks at the secret
 * properties the bound tree actually uses.
 */
final class InlineSecretCheck {

  /** One placeholder and nothing else; no {@code :default}, which would be an inline fallback. */
  private static final Pattern PLACEHOLDER_ONLY = Pattern.compile("\\$\\{[^${}:]+}");

  private InlineSecretCheck() {}

  /** {@code <property path>: <problem>} lines, one per literal secret. */
  static List<String> errors(
      KafkafimProperties properties, Iterable<ConfigurationPropertySource> sources) {
    List<String> names = secretPropertyNames(properties);
    List<String> errors = new ArrayList<>();
    for (String name : names) {
      ConfigurationProperty property = find(sources, name);
      if (property != null && !fromPlaceholderOrSecretFile(property)) {
        String origin = property.getOrigin() == null ? "" : " (" + property.getOrigin() + ")";
        errors.add(
            property.getName()
                + ": literal secret value"
                + origin
                + "; reference a mounted secret with a ${...} placeholder (DESIGN §5.5) or set "
                + KafkafimProperties.PREFIX
                + ".allow-inline-secrets=true for local development");
      }
    }
    return errors;
  }

  private static boolean fromPlaceholderOrSecretFile(ConfigurationProperty property) {
    if (property.getSource() != null
        && property.getSource().getUnderlyingSource() instanceof ConfigTreePropertySource) {
      return true;
    }
    return property.getValue() instanceof CharSequence raw
        && PLACEHOLDER_ONLY.matcher(raw.toString().strip()).matches();
  }

  private static ConfigurationProperty find(
      Iterable<ConfigurationPropertySource> sources, String path) {
    // Passthrough keys contain dots and may have been written as `properties[sasl.jaas.config]`.
    List<ConfigurationPropertyName> candidates = new ArrayList<>();
    for (String form : path.split("\\|")) {
      ConfigurationPropertyName name = ConfigurationPropertyName.ofIfValid(form);
      if (name != null) {
        candidates.add(name);
      }
    }
    for (ConfigurationPropertySource source : sources) {
      for (ConfigurationPropertyName name : candidates) {
        ConfigurationProperty property = source.getConfigurationProperty(name);
        if (property != null) {
          return property;
        }
      }
    }
    return null;
  }

  /**
   * Names of the secret-bearing properties that are set in the bound tree. Passthrough keys are
   * returned as {@code dotted|bracketed} alternatives.
   */
  static List<String> secretPropertyNames(KafkafimProperties properties) {
    List<String> names = new ArrayList<>();
    String kafka = KafkafimProperties.PREFIX + ".kafka.clusters.";
    properties
        .kafka()
        .clusters()
        .forEach((clusterName, cluster) -> {
          if (cluster == null) {
            return;
          }
          String clusterPath = kafka + clusterName;
          if (cluster.ssl() != null) {
            add(
                names,
                clusterPath + ".ssl.truststore-password",
                cluster.ssl().truststorePassword());
          }
          forEach(
              cluster.identities(),
              (identityName, identity) -> {
                String path = clusterPath + ".identities." + identityName;
                if (identity.sasl() != null) {
                  add(names, path + ".sasl.password", identity.sasl().password());
                }
                if (identity.ssl() != null) {
                  add(names, path + ".ssl.keystore-password", identity.ssl().keystorePassword());
                  add(names, path + ".ssl.key-password", identity.ssl().keyPassword());
                }
              });
          cluster
              .properties()
              .forEach((key, value) -> {
                if (isSecretPassthroughKey(key)) {
                  add(
                      names,
                      clusterPath
                          + ".properties."
                          + key
                          + "|"
                          + clusterPath
                          + ".properties["
                          + key
                          + "]",
                      value);
                }
              });
        });
    String elasticsearch = KafkafimProperties.PREFIX + ".elasticsearch.clusters.";
    properties
        .elasticsearch()
        .clusters()
        .forEach((clusterName, cluster) -> {
          if (cluster == null) {
            return;
          }
          forEach(
              cluster.identities(),
              (identityName, identity) -> {
                String path = elasticsearch + clusterName + ".identities." + identityName;
                add(names, path + ".password", identity.password());
                add(names, path + ".api-key", identity.apiKey());
                if (identity.ssl() != null) {
                  add(names, path + ".ssl.keystore-password", identity.ssl().keystorePassword());
                }
              });
        });
    return names;
  }

  /**
   * Passthrough kafka-clients keys that carry secrets: {@code *password} (for example {@code
   * ssl.key.password}), {@code *secret*} and {@code sasl.jaas.config}, which embeds credentials.
   */
  static boolean isSecretPassthroughKey(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.endsWith("password")
        || lower.contains("secret")
        || lower.equals("sasl.jaas.config");
  }

  private static void add(List<String> names, String name, Object boundValue) {
    if (boundValue != null) {
      names.add(name);
    }
  }

  private static <T> void forEach(Map<String, T> map, BiConsumer<String, T> action) {
    map.forEach((key, value) -> {
      if (value != null) {
        action.accept(key, value);
      }
    });
  }
}
