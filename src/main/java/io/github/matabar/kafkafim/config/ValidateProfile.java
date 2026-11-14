package io.github.matabar.kafkafim.config;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/**
 * The {@code validate} profile (DESIGN §5.6, ADR-0018, ADR-0040, ADR-0043): started with {@code
 * --spring.profiles.active=validate,kafkafim} and a rendered configuration mounted, the application
 * binds and validates {@code kafkafim.*}, logs a summary and exits 0, or fails startup (exit 1)
 * with every violation. No pipeline, client, web server or telemetry export is started. It is meant
 * for the infrastructure repository's CI.
 *
 * <p>A tree that defines no cluster and no pipeline also fails ({@link #requireConfiguration}): it
 * means the configuration was not found.
 *
 * <p>The checks are those of a normal start, with one exception: where the profile runs, the
 * Secrets are usually not mounted, so a credential placeholder that nothing defines is accepted and
 * the summary lists the secret names the configuration expects. Literal secrets are still rejected.
 */
public final class ValidateProfile {

  /** The profile's name. */
  public static final String NAME = "validate";

  private ValidateProfile() {}

  /**
   * Whether {@code environment} runs the {@code validate} profile.
   *
   * @param environment the application's environment
   * @return {@code true} when the profile is active
   */
  public static boolean isActive(Environment environment) {
    return environment.acceptsProfiles(Profiles.of(NAME));
  }

  /**
   * Refuses a tree that defines nothing: no Kafka cluster, no Elasticsearch cluster and no
   * pipeline. A normal start accepts it with a warning, but under this profile it almost always
   * means the configuration was not found (a wrong mount path, a missing {@code
   * application-kafkafim.yaml}, the {@code kafkafim} profile not active), and passing it would
   * report a configuration as valid that was never read. A tree with clusters and no pipelines is a
   * deliberately empty group and passes. The bound tree is what is checked, not "no {@code
   * kafkafim.*} property": the image's own {@code application.yaml} always sets {@code
   * kafkafim.reload.*}.
   *
   * @param properties the bound tree
   * @throws IllegalStateException if the tree defines no cluster and no pipeline
   */
  static void requireConfiguration(KafkafimProperties properties) {
    if (properties.pipelines().isEmpty()
        && properties.kafka().clusters().isEmpty()
        && properties.elasticsearch().clusters().isEmpty()) {
      throw new IllegalStateException(
          "No kafkafim configuration found: no Kafka cluster, no Elasticsearch cluster and no"
              + " pipeline is defined. Check that application-kafkafim.yaml is mounted where"
              + " the image reads it (/etc/kafkafim/config/), that the 'kafkafim' profile is"
              + " active (--spring.profiles.active=validate,kafkafim), and that the file"
              + " defines clusters or pipelines.");
    }
  }

  /**
   * What a successful validation logs.
   *
   * @param text several lines: the totals, one block per pipeline, the expected secrets; never
   *     contains a secret value
   */
  public record Summary(String text) {}

  /**
   * Describes a tree that bound and validated.
   *
   * @param properties the bound tree
   * @param sources the property sources it was bound from, for the expected secret names
   * @return the summary
   */
  static Summary summary(
      KafkafimProperties properties, Iterable<ConfigurationPropertySource> sources) {
    StringBuilder text = new StringBuilder("Configuration is valid; nothing was started. ");
    text.append(properties.pipelines().size())
        .append(" pipeline(s), ")
        .append(properties.kafka().clusters().size())
        .append(" Kafka cluster(s), ")
        .append(properties.elasticsearch().clusters().size())
        .append(" Elasticsearch cluster(s)");
    for (Map.Entry<String, Pipeline> entry : properties.pipelines().entrySet()) {
      appendPipeline(text, entry.getKey(), entry.getValue());
    }
    List<InlineSecretCheck.ExpectedSecret> secrets =
        InlineSecretCheck.expectedSecrets(properties, sources);
    text.append("\n  expected secrets (").append(secrets.size()).append(")");
    long unmounted = secrets.stream().filter(secret -> !secret.mounted()).count();
    if (unmounted > 0) {
      text.append(", ").append(unmounted).append(" not mounted here");
    }
    for (InlineSecretCheck.ExpectedSecret secret : secrets) {
      text.append("\n    ").append(secret.name());
      if (!secret.mounted()) {
        text.append(" (not mounted)");
      }
    }
    return new Summary(text.toString());
  }

  private static void appendPipeline(StringBuilder text, String name, Pipeline pipeline) {
    Pipeline.Source source = pipeline.source();
    text.append("\n  pipeline ")
        .append(name)
        .append(": Kafka cluster ")
        .append(source.cluster())
        .append(identity(source.identity()))
        .append(", topics ")
        .append(source.topics())
        .append(", group ")
        .append(source.groupId())
        .append("; DLQ topic ")
        .append(pipeline.dlq().topic())
        .append(identity(pipeline.dlq().identity()))
        .append("; route-mode ")
        .append(pipeline.routeMode().name().toLowerCase(Locale.ROOT));
    List<Pipeline.Route> routes = pipeline.routes();
    for (int i = 0; i < routes.size(); i++) {
      Pipeline.Route route = routes.get(i);
      text.append("\n    route ").append(route.label(i));
      if (route.isDefault()) {
        text.append(" (default)");
      }
      if (route.isDrop()) {
        text.append(": drop");
      } else {
        text.append(": Elasticsearch cluster ")
            .append(route.cluster())
            .append(identity(route.identity()))
            .append(", index ")
            .append(route.index());
        if (route.pipeline() != null) {
          text.append(", ingest pipeline ").append(route.pipeline());
        }
      }
    }
  }

  private static String identity(String identity) {
    return identity == null ? " (no identity)" : " as " + identity;
  }
}
