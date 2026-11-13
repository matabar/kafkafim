package io.github.matabar.kafkafim.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.boot.origin.Origin;
import org.springframework.boot.origin.OriginLookup;
import org.springframework.core.env.PropertySource;

/**
 * Secrets must come from mounted {@code Secret} files (DESIGN §5.5). A secret-bearing property is
 * accepted when its value is read straight from a {@code configtree} file, or when its raw value is
 * exactly one {@code ${name}} placeholder (no {@code :default}) whose chain of references ends in a
 * {@code configtree} file. Anything else is a literal secret: a value in YAML, an environment
 * variable or a system property, also when reached through a placeholder.
 *
 * <p>Detection reads <em>unresolved</em> values: the {@code Binder} resolves placeholders while
 * binding, so the {@link ConfigurationPropertySource} that wins for a name (the binder's own
 * precedence) still returns the raw text with its origin. Each {@code ${name}} is then followed to
 * the property source that wins for {@code name}.
 *
 * <p>Independently of {@code allow-inline-secrets}, a placeholder in a credential (secrets and
 * usernames) that nothing defines is always an error: the binder leaves unresolvable placeholders
 * as literal text, so a missing or misnamed secret file would otherwise start the pod with {@code
 * "${...}"} as its password. The one exception is the {@code validate} profile ({@link
 * ValidateProfile}): it starts nothing, and where it runs the secrets are usually not mounted, so
 * there such a placeholder is accepted and listed by {@link #expectedSecrets} instead.
 */
final class InlineSecretCheck {

  /** One placeholder and nothing else; no {@code :default}, which would be an inline fallback. */
  private static final Pattern PLACEHOLDER_ONLY = Pattern.compile("\\$\\{([^${}:]+)}");

  /** Bound on placeholder chains, so a cycle cannot loop forever (the binder rejects it anyway). */
  private static final int MAX_DEPTH = 10;

  private static final String REMEDY =
      "reference a mounted secret with a ${...} placeholder (DESIGN §5.5) or set "
          + KafkafimProperties.PREFIX
          + ".allow-inline-secrets=true for local development";

  private InlineSecretCheck() {}

  /** A credential property the bound tree uses; usernames are only checked for resolution. */
  private record Candidate(String path, boolean secret) {}

  /** Where a placeholder chain ends. */
  private record Resolution(Kind kind, String placeholder, String origin) {}

  private enum Kind {
    SECRET_FILE,
    LITERAL,
    UNRESOLVED
  }

  /**
   * A secret the configuration reads through a placeholder or straight from a secret file.
   *
   * @param name the property name a mounted secret file must provide
   * @param mounted whether a {@code configtree} file provides it
   */
  record ExpectedSecret(String name, boolean mounted) {}

  /**
   * {@code <property path>: <problem>} lines.
   *
   * @param allowInlineSecrets skip the literal-secret rule; unresolved placeholders still fail
   * @param allowUnmountedSecrets accept a placeholder that nothing defines ({@code validate}
   *     profile only); literal secrets are still rejected
   */
  static List<String> errors(
      KafkafimProperties properties,
      Iterable<ConfigurationPropertySource> sources,
      boolean allowInlineSecrets,
      boolean allowUnmountedSecrets) {
    List<String> errors = new ArrayList<>();
    for (Candidate candidate : candidates(properties)) {
      boolean literalsMatter = candidate.secret() && !allowInlineSecrets;
      ConfigurationProperty property = find(sources, candidate.path());
      if (property == null) {
        if (literalsMatter) {
          errors.add(
              candidate.path().split("\\|")[0]
                  + ": cannot find the property source of this secret to verify it is not"
                  + " inline; "
                  + REMEDY);
        }
        continue;
      }
      if (isSecretFile(property.getSource())) {
        continue;
      }
      String name = property.getName().toString();
      Matcher placeholder = placeholder(property.getValue());
      if (placeholder != null) {
        Resolution resolution = follow(placeholder.group(1), sources, 0);
        if (resolution.kind() == Kind.UNRESOLVED) {
          if (allowUnmountedSecrets) {
            continue;
          }
          errors.add(
              name
                  + ": placeholder ${"
                  + resolution.placeholder()
                  + "} does not resolve; is its secret mounted under the configtree import?");
        } else if (resolution.kind() == Kind.LITERAL && literalsMatter) {
          errors.add(
              name
                  + ": placeholder ${"
                  + resolution.placeholder()
                  + "} resolves to a literal value ("
                  + resolution.origin()
                  + "), not a mounted secret file; "
                  + REMEDY);
        }
      } else if (literalsMatter) {
        errors.add(
            name + ": literal secret value (" + origin(property.getOrigin()) + "); " + REMEDY);
      }
    }
    return errors;
  }

  /**
   * The secrets (and placeholder usernames) the bound tree expects from mounted files, by property
   * name, sorted, each once: those a credential reads straight from a {@code configtree} file and
   * those a {@code ${name}} placeholder chain ends in, mounted or not. Literal values are left out.
   */
  static List<ExpectedSecret> expectedSecrets(
      KafkafimProperties properties, Iterable<ConfigurationPropertySource> sources) {
    Map<String, Boolean> expected = new TreeMap<>();
    for (Candidate candidate : candidates(properties)) {
      ConfigurationProperty property = find(sources, candidate.path());
      if (property == null) {
        continue;
      }
      if (isSecretFile(property.getSource())) {
        expected.put(property.getName().toString(), true);
        continue;
      }
      Matcher placeholder = placeholder(property.getValue());
      if (placeholder == null) {
        continue;
      }
      Resolution resolution = follow(placeholder.group(1), sources, 0);
      if (resolution.kind() != Kind.LITERAL) {
        expected.merge(
            resolution.placeholder(), resolution.kind() == Kind.SECRET_FILE, Boolean::logicalOr);
      }
    }
    List<ExpectedSecret> secrets = new ArrayList<>();
    expected.forEach((name, mounted) -> secrets.add(new ExpectedSecret(name, mounted)));
    return secrets;
  }

  private static Resolution follow(
      String name, Iterable<ConfigurationPropertySource> sources, int depth) {
    for (ConfigurationPropertySource source : sources) {
      if (!(source.getUnderlyingSource() instanceof PropertySource<?> propertySource)) {
        continue;
      }
      Object value = propertySource.getProperty(name);
      if (value == null) {
        continue;
      }
      if (propertySource instanceof ConfigTreePropertySource) {
        return new Resolution(Kind.SECRET_FILE, name, null);
      }
      Matcher next = placeholder(value);
      if (next != null && depth < MAX_DEPTH) {
        return follow(next.group(1), sources, depth + 1);
      }
      Origin origin =
          propertySource instanceof OriginLookup<?> lookup ? originOf(lookup, name) : null;
      return new Resolution(
          Kind.LITERAL, name, origin != null ? origin.toString() : propertySource.getName());
    }
    return new Resolution(Kind.UNRESOLVED, name, null);
  }

  @SuppressWarnings("unchecked")
  private static Origin originOf(OriginLookup<?> lookup, String name) {
    return ((OriginLookup<String>) lookup).getOrigin(name);
  }

  private static boolean isSecretFile(ConfigurationPropertySource source) {
    return source != null && source.getUnderlyingSource() instanceof ConfigTreePropertySource;
  }

  private static Matcher placeholder(Object raw) {
    if (!(raw instanceof CharSequence text)) {
      return null;
    }
    Matcher matcher = PLACEHOLDER_ONLY.matcher(text.toString().strip());
    return matcher.matches() ? matcher : null;
  }

  private static String origin(Origin origin) {
    return origin == null ? "origin unknown" : origin.toString();
  }

  private static ConfigurationProperty find(
      Iterable<ConfigurationPropertySource> sources, String path) {
    // Passthrough keys contain dots and may have been written as `properties[sasl.jaas.config]`.
    List<ConfigurationPropertyName> names = new ArrayList<>();
    for (String form : path.split("\\|")) {
      ConfigurationPropertyName name = ConfigurationPropertyName.ofIfValid(form);
      if (name != null) {
        names.add(name);
      }
    }
    for (ConfigurationPropertySource source : sources) {
      for (ConfigurationPropertyName name : names) {
        ConfigurationProperty property = source.getConfigurationProperty(name);
        if (property != null) {
          return property;
        }
      }
    }
    return null;
  }

  /**
   * The credential properties that are set in the bound tree. Passthrough keys are returned as
   * {@code dotted|bracketed} alternatives.
   */
  private static List<Candidate> candidates(KafkafimProperties properties) {
    List<Candidate> names = new ArrayList<>();
    String kafka = KafkafimProperties.PREFIX + ".kafka.clusters.";
    forEach(
        properties.kafka().clusters(),
        (clusterName, cluster) -> {
          String clusterPath = kafka + clusterName;
          if (cluster.ssl() != null) {
            secret(
                names,
                clusterPath + ".ssl.truststore-password",
                cluster.ssl().truststorePassword());
          }
          forEach(
              cluster.identities(),
              (identityName, identity) -> {
                String path = clusterPath + ".identities." + identityName;
                if (identity.sasl() != null) {
                  username(names, path + ".sasl.username", identity.sasl().username());
                  secret(names, path + ".sasl.password", identity.sasl().password());
                }
                if (identity.ssl() != null) {
                  secret(names, path + ".ssl.keystore-password", identity.ssl().keystorePassword());
                  secret(names, path + ".ssl.key-password", identity.ssl().keyPassword());
                }
              });
          cluster
              .properties()
              .forEach((key, value) -> {
                if (isSecretPassthroughKey(key)) {
                  String passthrough = clusterPath + ".properties";
                  secret(
                      names, passthrough + "." + key + "|" + passthrough + "[" + key + "]", value);
                }
              });
        });
    String elasticsearch = KafkafimProperties.PREFIX + ".elasticsearch.clusters.";
    forEach(
        properties.elasticsearch().clusters(),
        (clusterName, cluster) ->
            forEach(
                cluster.identities(),
                (identityName, identity) -> {
                  String path = elasticsearch + clusterName + ".identities." + identityName;
                  username(names, path + ".username", identity.username());
                  secret(names, path + ".password", identity.password());
                  secret(names, path + ".api-key", identity.apiKey());
                  if (identity.ssl() != null) {
                    secret(
                        names, path + ".ssl.keystore-password", identity.ssl().keystorePassword());
                  }
                }));
    return names;
  }

  /**
   * Passthrough kafka-clients keys that carry secrets: {@code *password} (for example {@code
   * ssl.key.password}), {@code *secret*}, {@code sasl.jaas.config} (embeds credentials), {@code
   * ssl.keystore.key} (an inline PEM private key) and {@code *user.info} (basic-auth pairs).
   */
  static boolean isSecretPassthroughKey(String key) {
    String lower = key.toLowerCase(Locale.ROOT);
    return lower.endsWith("password")
        || lower.contains("secret")
        || lower.equals("sasl.jaas.config")
        || lower.endsWith("keystore.key")
        || lower.endsWith("user.info");
  }

  private static void secret(List<Candidate> names, String name, Object boundValue) {
    if (boundValue != null) {
      names.add(new Candidate(name, true));
    }
  }

  private static void username(List<Candidate> names, String name, Object boundValue) {
    if (boundValue != null) {
      names.add(new Candidate(name, false));
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
