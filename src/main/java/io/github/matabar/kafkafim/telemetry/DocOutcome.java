package io.github.matabar.kafkafim.telemetry;

import java.util.Locale;

/** How a document ended ({@code kafkafim_docs_total{outcome}}, DESIGN §10). */
public enum DocOutcome {
  /** Elasticsearch stored it (or, for {@code op: create}, already had it). */
  INDEXED,
  /** A newer external version was already stored; treated as success. */
  STALE,
  /** Its DLQ record was acknowledged. */
  DLQ;

  /** The label value. */
  public String label() {
    return name().toLowerCase(Locale.ROOT);
  }
}
