package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.config.Pipeline.Op;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One document to write with a bulk {@code index} or {@code create} action (DESIGN §4.4). The
 * supervisor builds it from a decoded and routed document; the sink never looks inside {@code
 * source}.
 *
 * @param index concrete target index (or data stream) name, already resolved
 * @param id {@code _id}, or {@code null} to let Elasticsearch generate one ({@code id-missing:
 *     auto})
 * @param op {@code index} or {@code create}
 * @param version external version ({@code version_type: external}), or {@code null}
 * @param ingestPipeline Elasticsearch ingest pipeline named on the route, or {@code null}
 * @param source the document's JSON bytes, copied verbatim into the bulk body; must not contain a
 *     line feed (the decoder guarantees this, DESIGN §4.5)
 * @param handle opaque completion handle handed back with the outcome (for example the record's
 *     offset and document index)
 * @param <H> type of the completion handle
 */
public record BulkAction<H>(
    String index,
    @Nullable String id,
    Op op,
    @Nullable Long version,
    @Nullable String ingestPipeline,
    byte[] source,
    H handle) {

  /** Elasticsearch's {@code _id} limit in UTF-8 bytes. */
  public static final int MAX_ID_BYTES = 512;

  /** Validates the action; a violation is a programming error in the caller. */
  public BulkAction {
    Objects.requireNonNull(index, "index");
    Objects.requireNonNull(op, "op");
    Objects.requireNonNull(source, "source");
    if (index.isEmpty()) {
      throw new IllegalArgumentException("index must not be empty");
    }
    if (version != null && version < 0) {
      throw new IllegalArgumentException("version must not be negative");
    }
    // Each of the following makes Elasticsearch reject the whole bulk request (400), which the
    // sink could only retry forever: reject it here, at the caller.
    if (version != null && op == Op.CREATE) {
      throw new IllegalArgumentException(
          "op create does not support external versions (Elasticsearch: use index)");
    }
    if (id != null) {
      int idBytes = id.getBytes(StandardCharsets.UTF_8).length;
      if (idBytes == 0 || idBytes > MAX_ID_BYTES) {
        throw new IllegalArgumentException("_id must be 1 to 512 UTF-8 bytes");
      }
    }
    if (ingestPipeline != null && ingestPipeline.isEmpty()) {
      throw new IllegalArgumentException("ingest pipeline must not be empty");
    }
    if (source.length == 0) {
      throw new IllegalArgumentException("source must not be empty");
    }
    for (byte b : source) {
      if (b == '\n') {
        throw new IllegalArgumentException("source must not contain a line feed (NDJSON)");
      }
    }
  }
}
