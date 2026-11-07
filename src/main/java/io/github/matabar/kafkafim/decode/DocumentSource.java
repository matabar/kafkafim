package io.github.matabar.kafkafim.decode;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * The {@code _source} bytes of one document: the document's own bytes as they appear in the Kafka
 * value, not a re-serialisation of the parsed tree.
 *
 * <p>DESIGN §8 indexes documents byte-for-byte as decoded, so key order, number spelling ({@code
 * 1.10}, {@code 1e3}, integers beyond 64 bits) and string escapes reach Elasticsearch exactly as
 * produced. For an object record the source is the value itself, for an array record each element's
 * slice of it; both are views on the record's byte array, with no copy. The one exception is a
 * slice containing a line feed between tokens (pretty-printed JSON): the bulk body is NDJSON, so
 * such a slice is copied once with all whitespace outside strings removed. That is lossless, since
 * JSON forbids raw control characters inside strings and the parser rejects them.
 *
 * <p>Cost: one extra scan of each document's bytes for {@code '\n'}, plus one copy for
 * pretty-printed documents; the record's byte array stays reachable while any of its documents is
 * in flight.
 */
public final class DocumentSource {

  private final byte[] bytes;
  private final int offset;
  private final int length;

  private DocumentSource(byte[] bytes, int offset, int length) {
    this.bytes = bytes;
    this.offset = offset;
    this.length = length;
  }

  /**
   * The source for {@code value[start, end)}, a complete JSON value already validated by the
   * parser.
   */
  static DocumentSource of(byte[] value, int start, int end) {
    for (int i = start; i < end; i++) {
      if (value[i] == '\n') {
        return compact(value, start, end);
      }
    }
    return new DocumentSource(value, start, end - start);
  }

  private static DocumentSource compact(byte[] value, int start, int end) {
    byte[] out = new byte[end - start];
    int n = 0;
    boolean inString = false;
    boolean escaped = false;
    for (int i = start; i < end; i++) {
      byte b = value[i];
      if (inString) {
        if (escaped) {
          escaped = false;
        } else if (b == '\\') {
          escaped = true;
        } else if (b == '"') {
          inString = false;
        }
      } else if (b == '"') {
        inString = true;
      } else if (b == ' ' || b == '\t' || b == '\n' || b == '\r') {
        continue;
      }
      out[n++] = b;
    }
    return new DocumentSource(out, 0, n);
  }

  /** Number of bytes. */
  public int length() {
    return length;
  }

  /** Writes the bytes to {@code out}, e.g. a bulk request body. */
  public void writeTo(OutputStream out) throws IOException {
    out.write(bytes, offset, length);
  }

  /** A read-only view of the bytes; no copy. */
  public ByteBuffer asByteBuffer() {
    return ByteBuffer.wrap(bytes, offset, length).slice().asReadOnlyBuffer();
  }

  /** A copy of the bytes. */
  public byte[] toByteArray() {
    return Arrays.copyOfRange(bytes, offset, offset + length);
  }
}
