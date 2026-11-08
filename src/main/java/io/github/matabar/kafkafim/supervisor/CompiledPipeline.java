package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.decode.Decoder;
import io.github.matabar.kafkafim.route.Router;
import io.github.matabar.kafkafim.sink.es.SinkSettings;
import java.util.Map;
import java.util.Objects;

/**
 * One pipeline's configuration compiled into the building blocks' runtime objects; immutable, built
 * by {@link PipelineMapping#compile}.
 *
 * @param name pipeline name
 * @param config the bound pipeline
 * @param decoder decodes record values
 * @param router selects routes per document
 * @param streams one entry per {@code (cluster, identity)} the routes use, in route order
 * @param op bulk operation for every action
 * @param timestampFieldSet whether {@code document.timestamp-field} is configured; when it is not,
 *     ingest time drives {@code %{+...}} patterns (and a replayed DLQ record's original timestamp
 *     is used, ADR-0027)
 */
public record CompiledPipeline(
    String name,
    Pipeline config,
    Decoder decoder,
    Router<RouteTarget> router,
    Map<RouteTarget.StreamKey, SinkSettings> streams,
    Pipeline.Op op,
    boolean timestampFieldSet) {

  /** Requires every component and copies the streams. */
  public CompiledPipeline {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(config, "config");
    Objects.requireNonNull(decoder, "decoder");
    Objects.requireNonNull(router, "router");
    Objects.requireNonNull(op, "op");
    streams = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(streams));
  }
}
