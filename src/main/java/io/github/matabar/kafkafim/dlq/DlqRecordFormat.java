package io.github.matabar.kafkafim.dlq;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.jspecify.annotations.Nullable;

/**
 * The DLQ record layout (DESIGN §9, ADR-0011).
 *
 * <ul>
 *   <li>Key and value: the original bytes, untouched; a {@code null} key or value stays {@code
 *       null}.
 *   <li>Headers: the original headers first, in their order, except those in the reserved {@value
 *       #PREFIX} namespace; then kafkafim's headers. A record replayed from a DLQ topic and
 *       dead-lettered again therefore carries exactly one, current, set of {@code kafkafim.*}
 *       headers: stale ones (say a {@code kafkafim.doc-index} the new failure does not have) are
 *       dropped, never appended. The previous failure stays readable at its own DLQ coordinates,
 *       which the new {@code kafkafim.source.*} headers point to.
 *   <li>Header values are UTF-8 strings: numbers in decimal, instants in ISO-8601 UTC ({@code
 *       2026-11-08T10:15:30.123Z}), {@value #ERROR_REASON} cut to {@value #MAX_REASON_BYTES} bytes
 *       on a code-point boundary. A header whose value is unknown is omitted, not sent empty.
 *   <li>Record timestamp: unset, so the producer stamps the send time. The original timestamp is in
 *       {@value #SOURCE_TIMESTAMP}. An original timestamp would let retention delete a record
 *       dead-lettered late (a backlog older than the DLQ topic's retention) as soon as it lands,
 *       and the broker rejects timestamps outside its {@code message.timestamp.*} window, which
 *       would block the partition for a value kafkafim cannot change.
 *   <li>Partition: unset, so the default partitioner spreads by key like the source topic did.
 * </ul>
 */
public final class DlqRecordFormat {

  /** The header namespace kafkafim owns on DLQ records. */
  public static final String PREFIX = "kafkafim.";

  /** Name of the pipeline that dead-lettered the record. */
  public static final String PIPELINE = PREFIX + "pipeline";

  /** {@code error.type} (DESIGN §7). */
  public static final String ERROR_TYPE = PREFIX + "error.type";

  /** Failure detail, e.g. the ES item error JSON; at most {@value #MAX_REASON_BYTES} bytes. */
  public static final String ERROR_REASON = PREFIX + "error.reason";

  /** Source topic. */
  public static final String SOURCE_TOPIC = PREFIX + "source.topic";

  /** Source partition, decimal. */
  public static final String SOURCE_PARTITION = PREFIX + "source.partition";

  /** Source offset, decimal. */
  public static final String SOURCE_OFFSET = PREFIX + "source.offset";

  /** Source record timestamp, ISO-8601 UTC; omitted when the record had none. */
  public static final String SOURCE_TIMESTAMP = PREFIX + "source.timestamp";

  /** Elasticsearch cluster the document was routed to; omitted if unrouted. */
  public static final String TARGET_CLUSTER = PREFIX + "target.cluster";

  /** Resolved index; omitted if unrouted or unresolved. */
  public static final String TARGET_INDEX = PREFIX + "target.index";

  /** Indexing attempts, decimal. */
  public static final String ATTEMPTS = PREFIX + "attempts";

  /** First failure time, ISO-8601 UTC. */
  public static final String FIRST_FAILURE_TS = PREFIX + "first-failure-ts";

  /** Position inside a JSON array record, decimal; omitted for whole-record failures. */
  public static final String DOC_INDEX = PREFIX + "doc-index";

  /** Upper bound of {@link #ERROR_REASON} in UTF-8 bytes (4 KiB). */
  public static final int MAX_REASON_BYTES = 4096;

  private DlqRecordFormat() {}

  /**
   * The record to produce for a request.
   *
   * @param topic the DLQ topic
   * @param pipeline the pipeline name
   * @param request the failure
   * @return the producer record: original key and value, headers as described above
   */
  public static ProducerRecord<byte[], byte[]> toProducerRecord(
      String topic, String pipeline, DlqRequest request) {
    return new ProducerRecord<>(
        topic, null, null, request.key(), request.value(), headers(pipeline, request));
  }

  /**
   * The DLQ record's headers: original ones outside {@value #PREFIX}, then kafkafim's.
   *
   * @param pipeline the pipeline name
   * @param request the failure
   * @return mutable headers, in order
   */
  public static RecordHeaders headers(String pipeline, DlqRequest request) {
    List<Header> out = new ArrayList<>(request.headers().size() + 12);
    for (Header header : request.headers()) {
      if (!isReserved(header.key())) {
        out.add(new RecordHeader(header.key(), header.value()));
      }
    }
    add(out, PIPELINE, pipeline);
    add(out, ERROR_TYPE, request.errorType());
    if (request.reason() != null) {
      out.add(new RecordHeader(ERROR_REASON, truncateUtf8(request.reason(), MAX_REASON_BYTES)));
    }
    add(out, SOURCE_TOPIC, request.sourceTopic());
    add(out, SOURCE_PARTITION, Integer.toString(request.sourcePartition()));
    add(out, SOURCE_OFFSET, Long.toString(request.sourceOffset()));
    add(out, SOURCE_TIMESTAMP, iso(request.sourceTimestamp()));
    add(out, TARGET_CLUSTER, request.targetCluster());
    add(out, TARGET_INDEX, request.targetIndex());
    add(out, ATTEMPTS, Integer.toString(request.attempts()));
    add(out, FIRST_FAILURE_TS, iso(request.firstFailure()));
    add(out, DOC_INDEX, request.docIndex() == null ? null : request.docIndex().toString());
    return new RecordHeaders(out);
  }

  /**
   * Whether a header name is in kafkafim's reserved namespace (case-insensitive, so a {@code
   * Kafkafim.Error.Type} from some other producer cannot pose as a second value either).
   *
   * @param key header name
   * @return whether the DLQ writer drops it from the original headers
   */
  public static boolean isReserved(@Nullable String key) {
    return key != null && key.toLowerCase(Locale.ROOT).startsWith(PREFIX);
  }

  /**
   * UTF-8 bytes of {@code text}, cut to at most {@code maxBytes} without splitting a code point.
   * Unpaired surrogates are encoded as {@code ?}, as {@link String#getBytes} does.
   *
   * @param text the text
   * @param maxBytes upper bound, at least 4
   * @return the encoded, possibly shortened, text
   */
  static byte[] truncateUtf8(String text, int maxBytes) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    if (bytes.length <= maxBytes) {
      return bytes;
    }
    int end = maxBytes;
    // bytes[end] is the first byte cut off; while it is a continuation byte (10xxxxxx), the code
    // point it belongs to started before the cut, so move the cut to that code point's start.
    while (end > 0 && (bytes[end] & 0xC0) == 0x80) {
      end--;
    }
    return Arrays.copyOf(bytes, end);
  }

  private static @Nullable String iso(@Nullable Instant instant) {
    return instant == null ? null : DateTimeFormatter.ISO_INSTANT.format(instant);
  }

  private static void add(List<Header> out, String key, @Nullable String value) {
    if (value != null) {
      out.add(new RecordHeader(key, value.getBytes(StandardCharsets.UTF_8)));
    }
  }
}
