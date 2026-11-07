package io.github.matabar.kafkafim.route;

import tools.jackson.core.JsonPointer;

/** Fail-fast checks shared by the spec records and the compile step. */
final class Specs {

  private Specs() {}

  static void require(boolean condition, String message) {
    if (!condition) {
      throw new IllegalArgumentException(message);
    }
  }

  /**
   * Compiles an RFC 6901 field pointer, with the same acceptance as the configuration check: it
   * starts with {@code /} and every {@code ~} is followed by {@code 0} or {@code 1}.
   */
  static JsonPointer pointer(String pointer) {
    require(pointer != null && pointer.startsWith("/"), "'" + pointer + "' is not a JSON pointer");
    for (int i = pointer.indexOf('~'); i >= 0; i = pointer.indexOf('~', i + 1)) {
      require(
          i + 1 < pointer.length()
              && (pointer.charAt(i + 1) == '0' || pointer.charAt(i + 1) == '1'),
          "'" + pointer + "' is not a valid JSON pointer: '~' must be followed by '0' or '1'");
    }
    return JsonPointer.compile(pointer);
  }
}
