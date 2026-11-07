package io.github.matabar.kafkafim.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Null-to-default helpers for the compact constructors of the configuration records. */
final class Defaults {

  private Defaults() {}

  static <T> T orDefault(T value, T fallback) {
    return value != null ? value : fallback;
  }

  /**
   * An unmodifiable copy that preserves order. {@code null} elements are kept so that validation
   * can report them instead of the binder failing with a bare {@link NullPointerException}.
   */
  static <T> List<T> list(List<T> values) {
    return values == null ? List.of() : Collections.unmodifiableList(new ArrayList<>(values));
  }

  /** An unmodifiable copy that preserves insertion order, which drives error message order. */
  static <V> Map<String, V> map(Map<String, V> values) {
    return values == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(values));
  }
}
