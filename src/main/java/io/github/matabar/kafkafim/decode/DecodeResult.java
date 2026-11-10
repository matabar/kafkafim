package io.github.matabar.kafkafim.decode;

import java.util.List;
import java.util.Objects;

/** What {@link Decoder#decode} made of one Kafka record value. */
public sealed interface DecodeResult {

  /**
   * The record as a whole is undecodable (null, empty, oversize, invalid JSON, a scalar or {@code
   * null}, trailing content): one DLQ record with the original bytes and no {@code
   * kafkafim.doc-index}.
   *
   * @param error always of type {@link ErrorType#DECODE}
   */
  record RecordFailure(DecodeError error) implements DecodeResult {

    /** Rejects nulls. */
    public RecordFailure {
      Objects.requireNonNull(error, "error");
    }
  }

  /**
   * The record is a JSON object (one outcome, index 0) or a JSON array (one outcome per element, in
   * order). An empty array yields no outcomes: the record is complete as soon as it is seen and its
   * offset is committable without any ES or DLQ write.
   *
   * @param array whether the value was an array, i.e. whether a DLQ record for one of its outcomes
   *     carries {@code kafkafim.doc-index}
   * @param outcomes one per document, each decoded, unindexable or failed on its own
   */
  record Documents(boolean array, List<DocumentOutcome> outcomes) implements DecodeResult {

    /** Copies {@code outcomes}. */
    public Documents {
      outcomes = List.copyOf(outcomes);
    }
  }
}
