/**
 * The Elasticsearch sink (DESIGN §4.2, §7, ADR-0005, ADR-0008): clients per {@code (cluster,
 * identity)}, a batcher and bounded bulk sender per {@code (pipeline, cluster, identity)}, the
 * NDJSON body builder, the streaming item parser, the {@link
 * io.github.matabar.kafkafim.sink.es.ErrorClassifier} table and a circuit breaker per cluster.
 * Depends on {@code config} only among kafkafim packages (ADR-0003); the supervisor maps decoded
 * and routed documents to {@link io.github.matabar.kafkafim.sink.es.BulkAction}s.
 */
@NullMarked
package io.github.matabar.kafkafim.sink.es;

import org.jspecify.annotations.NullMarked;
