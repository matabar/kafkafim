package io.github.matabar.kafkafim.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.bind.Name;
import org.springframework.util.unit.DataSize;

/**
 * One consumer group reading one or more topics from one Kafka cluster and routing every document
 * to one or more Elasticsearch targets, {@code kafkafim.pipelines.<name>} (DESIGN §5.2).
 *
 * <p>Defaults that depend only on the pipeline itself (DLQ topic, document settings) are applied
 * here; those that depend on its name or on cluster definitions (group id, identity resolution) are
 * applied by {@link KafkafimProperties}.
 *
 * @param source what to consume
 * @param document how to extract {@code _id}, version and timestamp
 * @param routes evaluated in order
 * @param routeMode {@code first} (default) or {@code all} (fan-out to every match)
 * @param dlq dead-letter topic on the source cluster
 * @param latency end-to-end latency overrides
 */
public record Pipeline(
    @Valid Source source,
    @Valid Document document,
    @NotEmpty List<@Valid Route> routes,
    RouteMode routeMode,
    @Valid Dlq dlq,
    @Valid Latency latency) {

  /** Suffix of the default DLQ topic, appended to the first source topic. */
  public static final String DLQ_TOPIC_SUFFIX = ".kafkafim-dlq";

  /** Applies the documented defaults that do not depend on the pipeline name or on clusters. */
  public Pipeline {
    source = Defaults.orDefault(source, Source.EMPTY);
    document = Defaults.orDefault(document, Document.DEFAULTS);
    routes = Defaults.list(routes);
    routeMode = Defaults.orDefault(routeMode, RouteMode.FIRST);
    dlq = Defaults.orDefault(dlq, Dlq.DEFAULTS);
    if (dlq.topic() == null && !source.topics().isEmpty() && source.topics().get(0) != null) {
      dlq = dlq.withTopic(source.topics().get(0) + DLQ_TOPIC_SUFFIX);
    }
    latency = Defaults.orDefault(latency, Latency.DEFAULTS);
  }

  /** How many matching routes a document is sent to. */
  public enum RouteMode {
    /** The first matching route, falling back to the default route. */
    FIRST,
    /** Every matching route (fan-out); the default route only if none matched. */
    ALL
  }

  /**
   * The Kafka side of a pipeline.
   *
   * @param cluster name of a {@code kafkafim.kafka.clusters} entry
   * @param topics topics to subscribe to; the first one names the default DLQ topic
   * @param groupId consumer group; default {@code kafkafim.<pipeline name>}
   * @param identity consumer identity; default: the cluster's {@code default-identity}
   * @param offsetReset where a new group starts; default {@code latest}
   * @param maxPollRecords records per poll; default 500
   * @param maxInFlightBytes memory bound per pipeline, fetching pauses above it; default 64MB
   * @param commitInterval offset commit cadence; default 1s
   * @param maxRecordBytes largest accepted record value; default 8MB
   */
  public record Source(
      @NotBlank String cluster,
      @NotEmpty List<String> topics,
      String groupId,
      String identity,
      OffsetReset offsetReset,
      @Positive Integer maxPollRecords,
      DataSize maxInFlightBytes,
      Duration commitInterval,
      DataSize maxRecordBytes) {

    static final Source EMPTY = new Source(null, null, null, null, null, null, null, null, null);

    /** Applies the documented defaults except the group id, which needs the pipeline name. */
    public Source {
      topics = Defaults.list(topics);
      offsetReset = Defaults.orDefault(offsetReset, OffsetReset.LATEST);
      maxPollRecords = Defaults.orDefault(maxPollRecords, 500);
      maxInFlightBytes = Defaults.orDefault(maxInFlightBytes, DataSize.ofMegabytes(64));
      commitInterval = Defaults.orDefault(commitInterval, Duration.ofSeconds(1));
      maxRecordBytes = Defaults.orDefault(maxRecordBytes, DataSize.ofMegabytes(8));
    }

    Source withGroupIdAndIdentity(String newGroupId, String newIdentity) {
      return new Source(
          cluster,
          topics,
          newGroupId,
          newIdentity,
          offsetReset,
          maxPollRecords,
          maxInFlightBytes,
          commitInterval,
          maxRecordBytes);
    }
  }

  /** Kafka {@code auto.offset.reset} for a group without committed offsets. */
  public enum OffsetReset {
    /** Start at the end of each partition (production migration default). */
    LATEST,
    /** Start at the beginning of each partition. */
    EARLIEST
  }

  /**
   * Field extraction for each decoded document (DESIGN §4.4, §4.5).
   *
   * @param idField JSON pointer to the {@code _id}; default {@code /id}
   * @param idMissing what to do without an id; default {@code dlq}
   * @param versionField JSON pointer to an external version; optional
   * @param onVersionConflict what to do on a version conflict; default {@code ignore}
   * @param timestampField JSON pointer to the event time for {@code %{+...}}; default ingest time
   * @param timestampFormat {@code iso-8601} (default), {@code epoch-millis}, {@code epoch-seconds}
   *     or {@code pattern:<DateTimeFormatter>}
   * @param onBadTimestamp what to do with a missing or unparsable timestamp; default {@code dlq}
   * @param op bulk operation; default {@code index}
   */
  public record Document(
      String idField,
      IdMissing idMissing,
      String versionField,
      OnVersionConflict onVersionConflict,
      String timestampField,
      String timestampFormat,
      OnBadTimestamp onBadTimestamp,
      Op op) {

    static final Document DEFAULTS = new Document(null, null, null, null, null, null, null, null);

    /** {@code timestamp-format} prefix for a custom {@code DateTimeFormatter} pattern. */
    public static final String PATTERN_PREFIX = "pattern:";

    /** Applies the documented defaults. */
    public Document {
      idField = Defaults.orDefault(idField, "/id");
      idMissing = Defaults.orDefault(idMissing, IdMissing.DLQ);
      onVersionConflict = Defaults.orDefault(onVersionConflict, OnVersionConflict.IGNORE);
      timestampFormat = Defaults.orDefault(timestampFormat, "iso-8601");
      onBadTimestamp = Defaults.orDefault(onBadTimestamp, OnBadTimestamp.DLQ);
      op = Defaults.orDefault(op, Op.INDEX);
    }
  }

  /** {@code document.id-missing}. */
  public enum IdMissing {
    /** Send the document to the DLQ with {@code error.type=missing_id}. */
    DLQ,
    /** Let Elasticsearch generate an id; duplicates on retry. */
    AUTO
  }

  /** {@code document.on-version-conflict}. */
  public enum OnVersionConflict {
    /** Count the item as {@code stale} and treat it as success. */
    IGNORE,
    /** Send the document to the DLQ. */
    DLQ
  }

  /** {@code document.on-bad-timestamp}. */
  public enum OnBadTimestamp {
    /** Send the document to the DLQ with {@code error.type=bad_timestamp}. */
    DLQ,
    /** Use the ingest time instead. */
    INGEST_TIME
  }

  /** Bulk operation. */
  public enum Op {
    /** Create or replace. */
    INDEX,
    /** Create only; required for data streams; a 409 counts as success. */
    CREATE
  }

  /**
   * An ordered routing rule (DESIGN §5.3).
   *
   * @param name label for metrics; optional, consumers fall back to the route's position
   * @param when conditions that must all hold; empty only for the default route or in {@code all}
   *     mode
   * @param isDefault {@code default: true}: taken when no other route matched
   * @param cluster name of a {@code kafkafim.elasticsearch.clusters} entry
   * @param identity ES identity; default: the cluster's {@code default-identity}
   * @param index Logstash-style index pattern ({@code %{field}}, {@code %{+yyyy.MM.dd}})
   * @param pipeline ES ingest pipeline; optional
   */
  public record Route(
      String name,
      List<@Valid Condition> when,
      @Name("default") Boolean isDefault,
      @NotBlank String cluster,
      String identity,
      @NotBlank String index,
      String pipeline) {

    /** Applies the documented defaults. */
    public Route {
      when = Defaults.list(when);
      isDefault = Defaults.orDefault(isDefault, Boolean.FALSE);
    }

    Route withIdentity(String newIdentity) {
      return new Route(name, when, isDefault, cluster, newIdentity, index, pipeline);
    }
  }

  /**
   * One {@code when} condition. Exactly one subject ({@code field}, {@code header} or {@code
   * topic}); a {@code field} takes exactly one operator, a {@code header} takes {@code equals}, a
   * {@code topic} takes none.
   *
   * @param field JSON pointer into the document
   * @param header Kafka record header name
   * @param topic source topic equality
   * @param equalTo {@code equals: v}
   * @param oneOf {@code in: [a, b]}
   * @param exists {@code exists: true|false}
   * @param prefix {@code prefix: s}
   * @param matches {@code matches: regex}, anchored
   */
  public record Condition(
      String field,
      String header,
      String topic,
      @Name("equals") String equalTo,
      @Name("in") List<String> oneOf,
      Boolean exists,
      String prefix,
      String matches) {}

  /**
   * Dead-letter topic on the source Kafka cluster (ADR-0011).
   *
   * @param topic default {@code <first topic>.kafkafim-dlq}
   * @param identity producer identity; default: the resolved source identity
   * @param maxAttempts per document, item-level retryable errors only; default 10
   * @param maxElapsed per document, item-level retryable errors only; default 10m
   */
  public record Dlq(
      String topic, String identity, @Positive Integer maxAttempts, Duration maxElapsed) {

    static final Dlq DEFAULTS = new Dlq(null, null, null, null);

    /** Applies the documented defaults except the topic, which needs the source topics. */
    public Dlq {
      maxAttempts = Defaults.orDefault(maxAttempts, 10);
      maxElapsed = Defaults.orDefault(maxElapsed, Duration.ofMinutes(10));
    }

    Dlq withTopic(String newTopic) {
      return new Dlq(newTopic, identity, maxAttempts, maxElapsed);
    }

    Dlq withIdentity(String newIdentity) {
      return new Dlq(topic, newIdentity, maxAttempts, maxElapsed);
    }
  }

  /**
   * Latency overrides.
   *
   * @param maxAge overrides the target cluster's {@code bulk.max-age} for this pipeline; optional
   */
  public record Latency(Duration maxAge) {

    static final Latency DEFAULTS = new Latency(null);
  }
}
