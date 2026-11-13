package io.github.matabar.kafkafim.telemetry;

import io.github.matabar.kafkafim.config.Pipeline;
import java.util.Map;

/**
 * Meter names and label keys of DESIGN §10. Micrometer names are dotted; the Prometheus registry
 * renders them with underscores and the usual suffixes ({@code _total} for counters, {@code
 * _seconds} for timers), e.g. {@code kafkafim.docs} as {@code kafkafim_docs_total}.
 */
public final class TelemetryNames {

  /** {@code kafkafim_records_consumed_total{pipeline,kafka_cluster,topic}}. */
  public static final String RECORDS_CONSUMED = "kafkafim.records.consumed";

  /** {@code kafkafim_bytes_consumed_total{pipeline,kafka_cluster,topic}}: key plus value. */
  public static final String BYTES_CONSUMED = "kafkafim.bytes.consumed";

  /** {@code kafkafim_docs_total{pipeline,kafka_cluster,route,outcome}}. */
  public static final String DOCS = "kafkafim.docs";

  /** {@code kafkafim_bulk_requests_total{pipeline,kafka_cluster,es_cluster,status}}. */
  public static final String BULK_REQUESTS = "kafkafim.bulk.requests";

  /** {@code kafkafim_bulk_latency_seconds{pipeline,kafka_cluster,es_cluster}} (histogram). */
  public static final String BULK_LATENCY = "kafkafim.bulk.latency";

  /** {@code kafkafim_bulk_docs{pipeline,kafka_cluster,es_cluster}} (histogram). */
  public static final String BULK_DOCS = "kafkafim.bulk.docs";

  /** {@code kafkafim_bulk_bytes{pipeline,kafka_cluster,es_cluster}} (histogram). */
  public static final String BULK_BYTES = "kafkafim.bulk.bytes";

  /** {@code kafkafim_item_errors_total{pipeline,kafka_cluster,es_cluster,error_type,retryable}}. */
  public static final String ITEM_ERRORS = "kafkafim.item.errors";

  /** {@code kafkafim_retries_total{pipeline,kafka_cluster,es_cluster,scope}}. */
  public static final String RETRIES = "kafkafim.retries";

  /** {@code kafkafim_breaker_state{es_cluster}}: 0 closed, 1 half-open, 2 open. */
  public static final String BREAKER_STATE = "kafkafim.breaker.state";

  /** {@code kafkafim_auth_failures_total{pipeline,kafka_cluster,es_cluster,identity}}. */
  public static final String AUTH_FAILURES = "kafkafim.auth.failures";

  /** {@code kafkafim_e2e_latency_seconds{pipeline,kafka_cluster}} (histogram). */
  public static final String E2E_LATENCY = "kafkafim.e2e.latency";

  /** {@code kafkafim_batch_flush_total{pipeline,kafka_cluster,es_cluster,reason}}. */
  public static final String BATCH_FLUSH = "kafkafim.batch.flush";

  /** {@code kafkafim_in_flight_bytes{pipeline,kafka_cluster}}. */
  public static final String IN_FLIGHT_BYTES = "kafkafim.in.flight.bytes";

  /** {@code kafkafim_assigned_partitions{pipeline,kafka_cluster}}. */
  public static final String ASSIGNED_PARTITIONS = "kafkafim.assigned.partitions";

  /** {@code kafkafim_kafka_connected{pipeline,kafka_cluster}}: 1 or 0. */
  public static final String KAFKA_CONNECTED = "kafkafim.kafka.connected";

  /**
   * {@code kafkafim_pipeline_pause_reason{pipeline,kafka_cluster,reason}}: 1 while the pipeline is
   * paused for a reason of that kind ({@link PauseReason}), else 0.
   */
  public static final String PAUSE_REASON = "kafkafim.pipeline.pause.reason";

  /** {@code kafkafim_dlq_records_total{pipeline,kafka_cluster,error_type}}. */
  public static final String DLQ_RECORDS = "kafkafim.dlq.records";

  /** Label key of the pipeline name. */
  public static final String PIPELINE = "pipeline";

  /** Label key of an Elasticsearch cluster. */
  public static final String ES_CLUSTER = "es_cluster";

  /**
   * {@code route} of a document that failed before a route was selected; config validation rejects
   * a route of that name.
   */
  public static final String NO_ROUTE = Pipeline.Route.NO_ROUTE_LABEL;

  /** {@code identity} of an unauthenticated {@code http} cluster. */
  public static final String NONE = "none";

  /** {@code status} of a bulk request that got no HTTP response. */
  public static final String IO_ERROR_STATUS = "io_error";

  private static final Map<String, String> DESCRIPTIONS =
      Map.ofEntries(
          Map.entry(RECORDS_CONSUMED, "Kafka records delivered by the source"),
          Map.entry(BYTES_CONSUMED, "Key plus value bytes of the Kafka records delivered"),
          Map.entry(DOCS, "Documents done, by outcome (one per routed copy or failed document)"),
          Map.entry(BULK_REQUESTS, "Bulk requests by HTTP status (io_error: no response)"),
          Map.entry(BULK_LATENCY, "Bulk request latency, from sending to the parsed response"),
          Map.entry(BULK_DOCS, "Actions per bulk request"),
          Map.entry(BULK_BYTES, "Uncompressed body size per bulk request"),
          Map.entry(ITEM_ERRORS, "Failed bulk items by Elasticsearch error type"),
          Map.entry(RETRIES, "Retryable failures of whole requests and of items"),
          Map.entry(
              BREAKER_STATE,
              "Circuit breaker per Elasticsearch cluster: 0 closed, 1 half-open, 2 open"),
          Map.entry(AUTH_FAILURES, "Elasticsearch 401/403 per cluster and identity"),
          Map.entry(
              E2E_LATENCY, "Elasticsearch acknowledgement time minus the Kafka record timestamp"),
          Map.entry(BATCH_FLUSH, "Sealed batches by the limit that sealed them"),
          Map.entry(IN_FLIGHT_BYTES, "Key plus value bytes delivered and not yet completed"),
          Map.entry(ASSIGNED_PARTITIONS, "Partitions assigned to this pod's consumers"),
          Map.entry(KAFKA_CONNECTED, "1 while the pipeline's consumers hold broker connections"),
          Map.entry(
              PAUSE_REASON,
              "1 while the pipeline is paused for this kind of supervisor reason, else 0"),
          Map.entry(DLQ_RECORDS, "DLQ records acknowledged by the broker, by error type"));

  private TelemetryNames() {}

  static String description(String name) {
    return DESCRIPTIONS.getOrDefault(name, "");
  }
}
