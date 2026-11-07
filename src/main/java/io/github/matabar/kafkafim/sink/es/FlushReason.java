package io.github.matabar.kafkafim.sink.es;

/** Why a batch was sealed and handed to a sender ({@code kafkafim_batch_flush_total{reason}}). */
public enum FlushReason {
  /** {@code bulk.max-docs} reached. */
  DOCS,
  /** {@code bulk.max-bytes} reached, or the next action would not fit. */
  BYTES,
  /** The oldest action waited {@code max-age}. */
  AGE,
  /** {@link BulkSink#flush()} or a close. */
  EXPLICIT
}
