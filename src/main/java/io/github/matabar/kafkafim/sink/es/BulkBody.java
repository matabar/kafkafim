package io.github.matabar.kafkafim.sink.es;

import io.github.matabar.kafkafim.config.Pipeline.Op;
import java.io.ByteArrayOutputStream;
import java.util.List;
import tools.jackson.core.JsonGenerator;
import tools.jackson.core.ObjectWriteContext;
import tools.jackson.core.json.JsonFactory;

/**
 * Builds the NDJSON {@code _bulk} body (ADR-0005): per action one metadata line and the source
 * bytes copied verbatim, each line terminated by {@code \n}. Only the metadata line is generated;
 * strings in it are escaped by Jackson. The body size of an action is {@link #size(byte[],
 * byte[])}, so batch accounting and the request body agree byte for byte.
 */
final class BulkBody {

  private static final JsonFactory JSON = JsonFactory.builder().build();

  private BulkBody() {}

  /**
   * The metadata line of an action, without the trailing line feed, for example {@code
   * {"index":{"_index":"orders","_id":"1","version":5,"version_type":"external"}}}.
   */
  static byte[] metadataLine(BulkAction<?> action) {
    ByteArrayOutputStream out = new ByteArrayOutputStream(96);
    try (JsonGenerator generator = JSON.createGenerator(ObjectWriteContext.empty(), out)) {
      generator.writeStartObject();
      generator.writeName(action.op() == Op.CREATE ? "create" : "index");
      generator.writeStartObject();
      generator.writeName("_index");
      generator.writeString(action.index());
      if (action.id() != null) {
        generator.writeName("_id");
        generator.writeString(action.id());
      }
      if (action.version() != null) {
        generator.writeName("version");
        generator.writeNumber(action.version());
        generator.writeName("version_type");
        generator.writeString("external");
      }
      if (action.ingestPipeline() != null) {
        generator.writeName("pipeline");
        generator.writeString(action.ingestPipeline());
      }
      generator.writeEndObject();
      generator.writeEndObject();
    }
    return out.toByteArray();
  }

  /** Bytes an action adds to the body: both lines and their line feeds. */
  static long size(byte[] metadataLine, byte[] source) {
    return (long) metadataLine.length + 1 + source.length + 1;
  }

  /**
   * The request body for the given entries, in order.
   *
   * @param entries metadata line and source per action
   * @param totalSize the sum of {@link #size(byte[], byte[])} over the entries
   */
  static byte[] build(List<? extends Entry> entries, long totalSize) {
    if (totalSize > Integer.MAX_VALUE - 8) {
      throw new IllegalArgumentException("bulk body too large: " + totalSize + " bytes");
    }
    byte[] body = new byte[(int) totalSize];
    int position = 0;
    for (Entry entry : entries) {
      final byte[] metadata = entry.metadataLine();
      final byte[] source = entry.source();
      System.arraycopy(metadata, 0, body, position, metadata.length);
      position += metadata.length;
      body[position++] = '\n';
      System.arraycopy(source, 0, body, position, source.length);
      position += source.length;
      body[position++] = '\n';
    }
    if (position != body.length) {
      throw new IllegalStateException("body size mismatch: " + position + " != " + body.length);
    }
    return body;
  }

  /** One action's two lines. */
  interface Entry {
    /** The metadata line without its line feed. */
    byte[] metadataLine();

    /** The source without its line feed. */
    byte[] source();
  }
}
