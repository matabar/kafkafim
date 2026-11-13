package io.github.matabar.kafkafim.telemetry;

import java.util.Set;

/**
 * Read-only view of one pipeline's Kafka source for gauges and health (DESIGN §10). The supervisor
 * implements it over whatever source the pipeline currently runs; while no source runs every value
 * is zero (or {@code false}). Implementations must be quick and must not throw.
 */
public interface SourceProbe {

  /** A probe of a pipeline without a running source. */
  SourceProbe NONE = new SourceProbe() {};

  /** Key plus value bytes delivered and not yet completed ({@code kafkafim_in_flight_bytes}). */
  default long inFlightBytes() {
    return 0;
  }

  /** Partitions assigned to this pod's consumers ({@code kafkafim_assigned_partitions}). */
  default int assignedPartitions() {
    return 0;
  }

  /**
   * Whether the source is started and its consumers hold broker connections ({@code
   * kafkafim_kafka_connected}, readiness).
   */
  default boolean connected() {
    return false;
  }

  /**
   * Whether the pipeline is started (running or paused); only started pipelines count for the
   * {@code kafka} readiness contributor.
   */
  default boolean started() {
    return false;
  }

  /**
   * The kinds of supervisor reason the pipeline is paused for right now ({@code
   * kafkafim_pipeline_pause_reason}); empty unless its state is {@code paused}.
   */
  default Set<PauseReason> pauseReasons() {
    return Set.of();
  }
}
