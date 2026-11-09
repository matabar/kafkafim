package io.github.matabar.kafkafim.config;

import io.github.matabar.kafkafim.config.KafkaCluster.KafkaIdentity;
import io.github.matabar.kafkafim.config.KafkaCluster.SecurityProtocol;
import io.github.matabar.kafkafim.config.Pipeline.Condition;
import io.github.matabar.kafkafim.config.Pipeline.Route;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Stream;
import org.springframework.util.unit.DataSize;

/**
 * Cross-reference and semantic validation of a bound {@link KafkafimProperties} tree (DESIGN §5,
 * §5.2.1, §5.3). Pure: it inspects the model and returns every violation at once, so a broken tree
 * is reported in one go, at startup and (later) on reload. Bean Validation covers the per-field
 * constraints; this covers everything that needs more than one field.
 */
public final class ConfigValidator {

  /** Cluster, identity and pipeline names: lower-case kebab-case (they appear in paths). */
  private static final Pattern NAME = Pattern.compile("[a-z0-9]([a-z0-9-]*[a-z0-9])?");

  /** Only digits: reserved for the position label of unnamed routes. */
  private static final Pattern POSITION_LABEL = Pattern.compile("[0-9]+");

  /** {@code host:port}, IPv6 in brackets, optional {@code PROTOCOL://} prefix. */
  private static final Pattern BOOTSTRAP_SERVER =
      Pattern.compile("(?:[A-Za-z_]+://)?(\\[[0-9A-Fa-f:.%]+]|[A-Za-z0-9._-]+):(\\d{1,5})");

  private static final Set<String> TIMESTAMP_FORMATS =
      Set.of("iso-8601", "epoch-millis", "epoch-seconds");

  private final List<String> errors = new ArrayList<>();
  private final List<String> warnings = new ArrayList<>();

  private ConfigValidator() {}

  /** Validates {@code properties}; never throws for an invalid tree. */
  public static Result validate(KafkafimProperties properties) {
    ConfigValidator validator = new ConfigValidator();
    validator.check(properties);
    return new Result(List.copyOf(validator.errors), List.copyOf(validator.warnings));
  }

  /**
   * Outcome of a validation.
   *
   * @param errors {@code <property path>: <problem>} lines; non-empty means the tree is invalid
   * @param warnings conditions that bind but deserve a startup warning
   */
  public record Result(List<String> errors, List<String> warnings) {

    /** Whether the tree is valid. */
    public boolean isValid() {
      return errors.isEmpty();
    }
  }

  private void check(KafkafimProperties properties) {
    String prefix = KafkafimProperties.PREFIX;
    properties
        .kafka()
        .clusters()
        .forEach((name, cluster) -> {
          String path = prefix + ".kafka.clusters." + name;
          checkName(path, "Kafka cluster", name);
          if (cluster != null) {
            checkKafkaCluster(path, cluster);
          }
        });
    properties
        .elasticsearch()
        .clusters()
        .forEach((name, cluster) -> {
          String path = prefix + ".elasticsearch.clusters." + name;
          checkName(path, "Elasticsearch cluster", name);
          if (cluster != null) {
            checkElasticsearchCluster(path, cluster);
          }
        });
    if (properties.pipelines().isEmpty()) {
      warnings.add(prefix + ".pipelines: no pipelines configured; nothing will be consumed");
    }
    Map<String, String> groupOwners = new HashMap<>();
    properties
        .pipelines()
        .forEach((name, pipeline) -> {
          String path = prefix + ".pipelines." + name;
          checkName(path, "pipeline", name);
          if (pipeline != null) {
            checkPipeline(path, pipeline, properties);
            checkGroupUnique(path, name, pipeline.source(), groupOwners);
          }
        });
    KafkafimProperties.Reload reload = properties.reload();
    if (reload.pollInterval().isNegative() || reload.pollInterval().isZero()) {
      error(prefix + ".reload.poll-interval", "must be positive");
    }
    if (reload.debounce().isNegative()) {
      error(prefix + ".reload.debounce", "must not be negative");
    }
    if (properties.allowInlineSecrets()) {
      warnings.add(
          prefix + ".allow-inline-secrets=true: literal secrets accepted (local development only)");
    }
  }

  // ---------------------------------------------------------------- Kafka clusters

  private void checkKafkaCluster(String path, KafkaCluster cluster) {
    for (int i = 0; i < cluster.bootstrapServers().size(); i++) {
      String server = cluster.bootstrapServers().get(i);
      var matcher = server == null ? null : BOOTSTRAP_SERVER.matcher(server.trim());
      if (matcher == null || !matcher.matches() || !validPort(matcher.group(2))) {
        error(path + ".bootstrap-servers[" + i + "]", "'" + server + "' is not host:port");
      }
    }
    SecurityProtocol protocol = cluster.securityProtocol();
    if (cluster.ssl() != null && !protocol.usesTls()) {
      error(path + ".ssl", "trust is set but security-protocol " + protocol + " does not use TLS");
    }
    checkDefaultIdentity(path, cluster.defaultIdentity(), cluster.identities());
    cluster
        .identities()
        .forEach((name, identity) -> {
          String identityPath = path + ".identities." + name;
          checkName(identityPath, "identity", name);
          checkKafkaIdentity(identityPath, identity, protocol);
        });
  }

  private void checkKafkaIdentity(String path, KafkaIdentity identity, SecurityProtocol protocol) {
    if (identity == null || (identity.sasl() == null && identity.ssl() == null)) {
      error(path, "a Kafka identity needs a sasl block, an ssl keystore block, or both");
      return;
    }
    if (identity.sasl() != null) {
      if (!protocol.requiresSasl()) {
        error(path + ".sasl", "sasl is set but security-protocol " + protocol + " is not SASL_*");
      }
      if (identity.sasl().needsUsernameAndPassword()) {
        if (isBlank(identity.sasl().username())) {
          error(path + ".sasl.username", identity.sasl().mechanism() + " needs a username");
        }
        if (isBlank(identity.sasl().password())) {
          error(path + ".sasl.password", identity.sasl().mechanism() + " needs a password");
        }
      }
    }
    if (identity.ssl() != null && !protocol.usesTls()) {
      error(
          path + ".ssl",
          "a client keystore is set but security-protocol " + protocol + " does not use TLS");
    }
  }

  // ---------------------------------------------------------------- Elasticsearch clusters

  private void checkElasticsearchCluster(String path, ElasticsearchCluster cluster) {
    Set<String> schemes = new HashSet<>();
    for (int i = 0; i < cluster.uris().size(); i++) {
      String uri = cluster.uris().get(i);
      String uriPath = path + ".uris[" + i + "]";
      try {
        URI parsed = new URI(uri == null ? "" : uri);
        String scheme = parsed.getScheme();
        if (!"http".equals(scheme) && !"https".equals(scheme)) {
          error(uriPath, "'" + uri + "' must start with http:// or https://");
        } else if (parsed.getHost() == null) {
          error(uriPath, "'" + uri + "' has no host");
        } else {
          schemes.add(scheme);
        }
      } catch (URISyntaxException e) {
        error(uriPath, "'" + uri + "' is not a URI: " + e.getMessage());
      }
    }
    if (schemes.size() > 1) {
      error(path + ".uris", "mixes http and https; use one scheme per cluster");
    }
    boolean https = schemes.equals(Set.of("https"));
    if (cluster.ssl() != null && !https) {
      error(path + ".ssl", "trust is set but the cluster URIs are not https");
    }
    checkDefaultIdentity(path, cluster.defaultIdentity(), cluster.identities());
    cluster
        .identities()
        .forEach((name, identity) -> {
          String identityPath = path + ".identities." + name;
          checkName(identityPath, "identity", name);
          checkElasticsearchIdentity(identityPath, identity, https);
        });
    ElasticsearchCluster.Bulk bulk = cluster.bulk();
    checkPositive(path + ".bulk.max-bytes", bulk.maxBytes());
    checkPositive(path + ".bulk.max-age", bulk.maxAge());
    ElasticsearchCluster.Client client = cluster.client();
    checkPositive(path + ".client.connect-timeout", client.connectTimeout());
    checkPositive(path + ".client.response-timeout", client.responseTimeout());
  }

  private void checkElasticsearchIdentity(
      String path, ElasticsearchCluster.ElasticsearchIdentity identity, boolean https) {
    if (identity == null) {
      error(path, "an Elasticsearch identity needs username+password, api-key or an ssl keystore");
      return;
    }
    boolean basic = identity.hasBasic();
    boolean apiKey = identity.apiKey() != null;
    boolean keystore = identity.ssl() != null;
    if (basic && apiKey) {
      error(path, "set either username+password or api-key, not both");
    } else if (!basic && !apiKey && !keystore) {
      error(path, "an Elasticsearch identity needs username+password, api-key or an ssl keystore");
    }
    if (basic && isBlank(identity.username())) {
      error(path + ".username", "basic auth needs a username");
    }
    if (basic && isBlank(identity.password())) {
      error(path + ".password", "basic auth needs a password");
    }
    if (apiKey && isBlank(identity.apiKey())) {
      error(path + ".api-key", "api-key is empty");
    }
    if (keystore && !https) {
      error(path + ".ssl", "a client keystore needs https cluster URIs");
    }
  }

  // ---------------------------------------------------------------- pipelines

  private void checkPipeline(String path, Pipeline pipeline, KafkafimProperties properties) {
    Pipeline.Source source = pipeline.source();
    KafkaCluster kafkaCluster = null;
    if (source.cluster() != null) {
      kafkaCluster = properties.kafka().clusters().get(source.cluster());
      if (kafkaCluster == null) {
        error(path + ".source.cluster", "unknown Kafka cluster '" + source.cluster() + "'");
      }
    }
    checkTopics(path + ".source.topics", source.topics());
    if (isBlank(source.groupId())) {
      error(path + ".source.group-id", "must not be blank");
    }
    checkPositive(path + ".source.max-in-flight-bytes", source.maxInFlightBytes());
    checkPositive(path + ".source.commit-interval", source.commitInterval());
    checkPositive(path + ".source.max-record-bytes", source.maxRecordBytes());
    if (source.maxRecordBytes().compareTo(source.maxInFlightBytes()) > 0) {
      error(
          path + ".source.max-record-bytes",
          "must not exceed max-in-flight-bytes ("
              + source.maxInFlightBytes()
              + "), or one record could stall the pipeline");
    }
    if (kafkaCluster != null) {
      checkKafkaIdentityReference(
          path + ".source.identity", source.identity(), source.cluster(), kafkaCluster);
    }

    checkDocument(path + ".document", pipeline.document());
    checkRoutes(path, pipeline, properties);

    Pipeline.Dlq dlq = pipeline.dlq();
    if (isBlank(dlq.topic())) {
      error(path + ".dlq.topic", "must not be blank");
    } else if (source.topics().contains(dlq.topic())) {
      error(path + ".dlq.topic", "'" + dlq.topic() + "' is also a source topic of this pipeline");
    }
    checkPositive(path + ".dlq.max-elapsed", dlq.maxElapsed());
    // Same identity on the same cluster as the source: already checked there, report it once.
    if (kafkaCluster != null && !Objects.equals(dlq.identity(), source.identity())) {
      checkKafkaIdentityReference(
          path + ".dlq.identity", dlq.identity(), source.cluster(), kafkaCluster);
    }
    if (pipeline.latency().maxAge() != null) {
      checkPositive(path + ".latency.max-age", pipeline.latency().maxAge());
    }
  }

  private void checkTopics(String path, List<String> topics) {
    Set<String> seen = new HashSet<>();
    for (int i = 0; i < topics.size(); i++) {
      String topic = topics.get(i);
      if (isBlank(topic)) {
        error(path + "[" + i + "]", "must not be blank");
      } else if (!seen.add(topic)) {
        error(path + "[" + i + "]", "'" + topic + "' is listed twice");
      }
    }
  }

  private void checkGroupUnique(
      String path, String name, Pipeline.Source source, Map<String, String> groupOwners) {
    if (source.cluster() == null || source.groupId() == null) {
      return;
    }
    String key = source.cluster() + '\u0000' + source.groupId();
    String owner = groupOwners.putIfAbsent(key, name);
    if (owner != null) {
      error(
          path + ".source.group-id",
          "consumer group '"
              + source.groupId()
              + "' on Kafka cluster '"
              + source.cluster()
              + "' is already used by pipeline '"
              + owner
              + "'; a consumer group belongs to exactly one pipeline");
    }
  }

  private void checkKafkaIdentityReference(
      String path, String identity, String clusterName, KafkaCluster cluster) {
    SecurityProtocol protocol = cluster.securityProtocol();
    if (identity == null) {
      // An invalid default-identity is reported on the cluster; do not send users here too.
      if (protocol.requiresSasl() && cluster.defaultIdentity() == null) {
        error(
            path,
            "Kafka cluster '"
                + clusterName
                + "' uses "
                + protocol
                + " and no identity resolves; set this identity or the cluster's"
                + " default-identity");
      }
      return;
    }
    KafkaIdentity resolved = cluster.identities().get(identity);
    if (resolved == null) {
      error(path, "unknown identity '" + identity + "' on Kafka cluster '" + clusterName + "'");
    } else if (protocol.requiresSasl() && resolved.sasl() == null) {
      error(
          path,
          "identity '"
              + identity
              + "' has no sasl block but Kafka cluster '"
              + clusterName
              + "' uses "
              + protocol);
    }
  }

  private void checkDocument(String path, Pipeline.Document document) {
    pointer(path + ".id-field", document.idField());
    if (document.versionField() != null) {
      pointer(path + ".version-field", document.versionField());
      if (document.op() == Pipeline.Op.CREATE) {
        // Elasticsearch: "create operations only support internal versioning"; BulkAction rejects
        // the pair too, but only at runtime.
        error(
            path + ".op",
            "op create does not support external versions; remove document.version-field or"
                + " use document.op: index");
      }
    }
    if (document.timestampField() != null) {
      pointer(path + ".timestamp-field", document.timestampField());
    }
    String format = document.timestampFormat();
    if (format.startsWith(Pipeline.Document.PATTERN_PREFIX)) {
      String pattern = format.substring(Pipeline.Document.PATTERN_PREFIX.length());
      List<String> problems = TimestampPatternSyntax.errors(pattern);
      for (String problem : problems) {
        error(path + ".timestamp-format", problem);
      }
      if (problems.isEmpty()) {
        for (String warning : TimestampPatternSyntax.warnings(pattern)) {
          warnings.add(path + ".timestamp-format: " + warning);
        }
      }
    } else if (!TIMESTAMP_FORMATS.contains(format)) {
      error(
          path + ".timestamp-format",
          "'"
              + format
              + "' is not one of iso-8601, epoch-millis, epoch-seconds,"
              + " pattern:<DateTimeFormatter>");
    }
    if (document.idMissing() == Pipeline.IdMissing.AUTO) {
      warnings.add(
          path
              + ".id-missing=auto: documents without an id get ES-generated ids and are"
              + " duplicated on retry");
    }
  }

  // ---------------------------------------------------------------- routes

  private void checkRoutes(String path, Pipeline pipeline, KafkafimProperties properties) {
    List<Route> routes = pipeline.routes();
    boolean firstMode = pipeline.routeMode() == Pipeline.RouteMode.FIRST;
    int defaults = 0;
    Map<String, Integer> names = new HashMap<>();
    for (int i = 0; i < routes.size(); i++) {
      Route route = routes.get(i);
      String routePath = path + ".routes[" + i + "]";
      if (route == null) {
        error(routePath, "must not be empty");
        continue;
      }
      checkRouteName(routePath + ".name", route.name(), i, names);
      if (route.isDefault()) {
        defaults++;
        if (i != routes.size() - 1) {
          error(routePath + ".default", "the default route must be the last route");
        }
        if (!route.when().isEmpty()) {
          error(routePath + ".when", "the default route takes no conditions");
        }
      } else if (firstMode && route.when().isEmpty()) {
        error(
            routePath + ".when",
            "a route without conditions matches everything and hides the routes after it;"
                + " use default: true, or route-mode: all for unconditional fan-out");
      }
      for (int c = 0; c < route.when().size(); c++) {
        checkCondition(routePath + ".when[" + c + "]", route.when().get(c), pipeline);
      }
      checkRouteTarget(routePath, route, properties);
      if (route.pipeline() != null && route.pipeline().isBlank()) {
        // Elasticsearch rejects the whole bulk request for an empty ingest pipeline name.
        error(
            routePath + ".pipeline",
            "must not be blank; omit it to index without an ingest pipeline");
      }
      if (route.index() != null) {
        for (String problem : IndexPatternSyntax.errors(route.index())) {
          error(routePath + ".index", problem);
        }
        for (String warning : IndexPatternSyntax.warnings(route.index())) {
          warnings.add(routePath + ".index: " + warning);
        }
      }
    }
    if (firstMode && defaults != 1 && !routes.isEmpty()) {
      error(
          path + ".routes",
          "route-mode first needs exactly one default: true route, found " + defaults);
    } else if (!firstMode && defaults > 1) {
      error(path + ".routes", "at most one default: true route is allowed, found " + defaults);
    }
  }

  /**
   * A route's name is its {@code route} label; an unnamed route is labelled by its position and a
   * document that failed before routing by {@value Route#NO_ROUTE_LABEL}. Rejects every name that
   * would make two of those labels collide.
   */
  private void checkRouteName(String path, String name, int position, Map<String, Integer> names) {
    if (name == null) {
      return;
    }
    if (name.isBlank()) {
      error(path, "must not be blank; omit it to label the route by its position");
    } else if (name.equals(Route.NO_ROUTE_LABEL)) {
      error(
          path,
          "'"
              + Route.NO_ROUTE_LABEL
              + "' is reserved: it labels documents that failed before a route was selected");
    } else if (POSITION_LABEL.matcher(name).matches()) {
      error(
          path,
          "'" + name + "' is reserved: digits-only labels are the positions of unnamed routes");
    } else {
      Integer first = names.putIfAbsent(name, position);
      if (first != null) {
        error(path, "'" + name + "' is already the name of routes[" + first + "]");
      }
    }
  }

  private void checkRouteTarget(String path, Route route, KafkafimProperties properties) {
    if (route.cluster() == null) {
      return;
    }
    ElasticsearchCluster cluster = properties.elasticsearch().clusters().get(route.cluster());
    if (cluster == null) {
      error(path + ".cluster", "unknown Elasticsearch cluster '" + route.cluster() + "'");
      return;
    }
    if (route.identity() == null) {
      if (cluster.defaultIdentity() != null) {
        return; // the cluster's default-identity is invalid and reported there
      } else if (cluster.usesHttps()) {
        error(
            path + ".identity",
            "Elasticsearch cluster '"
                + route.cluster()
                + "' uses https and no identity resolves; set this identity or the cluster's"
                + " default-identity");
      } else {
        warnings.add(
            path
                + ": no identity for Elasticsearch cluster '"
                + route.cluster()
                + "' over plain http; requests are unauthenticated (local development only)");
      }
    } else if (!cluster.identities().containsKey(route.identity())) {
      error(
          path + ".identity",
          "unknown identity '"
              + route.identity()
              + "' on Elasticsearch cluster '"
              + route.cluster()
              + "'");
    }
  }

  private void checkCondition(String path, Condition condition, Pipeline pipeline) {
    if (condition == null) {
      error(path, "must not be empty");
      return;
    }
    List<String> operators = new ArrayList<>();
    if (condition.equalTo() != null) {
      operators.add("equals");
    }
    if (condition.oneOf() != null) {
      operators.add("in");
    }
    if (condition.exists() != null) {
      operators.add("exists");
    }
    if (condition.prefix() != null) {
      operators.add("prefix");
    }
    if (condition.matches() != null) {
      operators.add("matches");
    }
    long subjects =
        Stream.of(condition.field(), condition.header(), condition.topic())
            .filter(Objects::nonNull)
            .count();
    if (subjects != 1) {
      error(path, "needs exactly one of field, header or topic, found " + subjects);
      return;
    }
    if (condition.field() != null) {
      pointer(path + ".field", condition.field());
      if (operators.size() != 1) {
        error(
            path,
            "a field condition needs exactly one of equals, in, exists, prefix or matches, found "
                + (operators.isEmpty() ? "none" : String.join(", ", operators)));
      }
      if (condition.oneOf() != null && condition.oneOf().isEmpty()) {
        error(path + ".in", "must list at least one value");
      }
      if (condition.matches() != null) {
        try {
          Pattern.compile(condition.matches());
        } catch (PatternSyntaxException e) {
          error(path + ".matches", "invalid regex: " + e.getDescription());
        }
      }
    } else if (condition.header() != null) {
      if (condition.header().isBlank()) {
        error(path + ".header", "must not be blank");
      }
      if (!operators.equals(List.of("equals"))) {
        error(path, "a header condition takes exactly one operator, equals");
      }
    } else {
      if (!operators.isEmpty()) {
        error(path, "a topic condition takes no operator, found " + String.join(", ", operators));
      }
      if (!pipeline.source().topics().contains(condition.topic())) {
        error(
            path + ".topic",
            "'" + condition.topic() + "' is not one of this pipeline's source topics");
      }
    }
  }

  // ---------------------------------------------------------------- helpers

  private void checkDefaultIdentity(
      String path, String defaultIdentity, Map<String, ?> identities) {
    if (defaultIdentity != null && !identities.containsKey(defaultIdentity)) {
      error(
          path + ".default-identity",
          "'"
              + defaultIdentity
              + "' is not one of this cluster's identities "
              + identities.keySet());
    }
  }

  private void checkName(String path, String kind, String name) {
    if (!NAME.matcher(name).matches()) {
      error(
          path,
          kind
              + " name '"
              + name
              + "' must be lower-case letters, digits and '-' (it appears in group ids,"
              + " metrics and secret paths)");
    }
  }

  private void pointer(String path, String pointer) {
    String problem = JsonPointers.error(pointer);
    if (problem != null) {
      error(path, problem);
    }
  }

  private void checkPositive(String path, DataSize size) {
    if (size != null && size.toBytes() <= 0) {
      error(path, "must be positive");
    }
  }

  private void checkPositive(String path, Duration duration) {
    if (duration != null && (duration.isZero() || duration.isNegative())) {
      error(path, "must be positive");
    }
  }

  private static boolean validPort(String port) {
    int value = Integer.parseInt(port);
    return value > 0 && value <= 65535;
  }

  private static boolean isBlank(String value) {
    return value == null || value.isBlank();
  }

  private void error(String path, String message) {
    errors.add(path + ": " + message);
  }
}
