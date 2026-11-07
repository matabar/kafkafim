package io.github.matabar.kafkafim.dlq;

/**
 * Completes a DLQ send that had not been acknowledged when the writer closed. The caller must not
 * complete the source offset: the record stays uncommitted and is re-delivered (ADR-0006).
 */
public final class DlqClosedException extends IllegalStateException {

  private static final long serialVersionUID = 1L;

  DlqClosedException(String message) {
    super(message);
  }
}
