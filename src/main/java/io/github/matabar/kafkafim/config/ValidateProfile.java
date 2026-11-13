package io.github.matabar.kafkafim.config;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/**
 * The {@code validate} profile (DESIGN §5.6, ADR-0018): started with {@code
 * --spring.profiles.active=validate,kafkafim} and a rendered configuration mounted, the application
 * binds and validates {@code kafkafim.*}, logs a summary and exits 0, or fails startup (exit 1)
 * with every violation. No pipeline, client, web server or telemetry export is started. It is meant
 * for the infrastructure repository's CI.
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
