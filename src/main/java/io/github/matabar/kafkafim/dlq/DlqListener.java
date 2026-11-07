package io.github.matabar.kafkafim.dlq;

import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Observes DLQ writers, for metrics (the telemetry slice adds {@code
 * kafkafim_dlq_records_total{pipeline,error_type}} from {@link #onWritten}) and for the supervisor,
 * which pauses a pipeline while its writer is blocked and alerts.
 *
 * <p>Callbacks run on kafkafim's DLQ threads, never on the caller's or the producer's I/O thread;
 * they must be quick and must not throw (an exception is logged and ignored).
 */
public interface DlqListener {

  /** A listener that does nothing. */
  DlqListener NONE = new DlqListener() {};

  /**
   * A DLQ record was acknowledged by the broker; called before the send's completion.
   *
   * @param pipeline the pipeline name
   * @param request the dead-lettered document
   * @param metadata where the DLQ record landed
   */
  default void onWritten(String pipeline, DlqRequest request, RecordMetadata metadata) {}

  /**
   * A send attempt failed and will be retried.
   *
   * @param pipeline the pipeline name
   * @param request the document being dead-lettered
   * @param failure the failure, its kind and the retry delay
   */
  default void onSendFailed(String pipeline, DlqRequest request, DlqFailure failure) {}

  /**
   * The pipeline's writer became blocked: a send failed and none of its failing sends has succeeded
   * since. The affected source offsets cannot be committed (the supervisor should pause the
   * pipeline and alert).
   *
   * @param pipeline the pipeline name
   * @param failure the failure that blocked it
   */
  default void onBlocked(String pipeline, DlqFailure failure) {}

  /**
   * Every send that had failed has now been acknowledged; the writer is no longer blocked.
   *
   * @param pipeline the pipeline name
   */
  default void onUnblocked(String pipeline) {}
}
