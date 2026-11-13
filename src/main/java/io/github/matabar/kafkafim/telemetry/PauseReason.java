package io.github.matabar.kafkafim.telemetry;

/**
 * Why the supervisor paused a pipeline, reduced to its kind: the {@code reason} label of {@code
 * kafkafim_pipeline_pause_reason} (DESIGN §10). The cluster and identity a reason names stay in the
 * logs, so every pipeline has the same three series.
 */
public enum PauseReason {
  /** The circuit breaker of an Elasticsearch cluster the pipeline routes to is not closed. */
  ES_BREAKER("es_breaker"),
  /** Elasticsearch rejects an identity the pipeline writes with (401/403). */
  ES_AUTH("es_auth"),
  /** A DLQ record of the pipeline could not be produced yet. */
  DLQ_BLOCKED("dlq_blocked");

  private final String label;

  PauseReason(String label) {
    this.label = label;
  }

  /** The {@code reason} label value. */
  public String label() {
    return label;
  }
}
