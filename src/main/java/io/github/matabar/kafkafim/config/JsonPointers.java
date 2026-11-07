package io.github.matabar.kafkafim.config;

import tools.jackson.core.JsonPointer;

/** JSON pointer (RFC 6901) checks for configured field references. */
final class JsonPointers {

  private JsonPointers() {}

  /**
   * Why {@code pointer} is not a usable field pointer, or {@code null} if it is. The root pointer
   * ({@code ""}) is rejected because every reference names a field inside a document.
   */
  static String error(String pointer) {
    if (pointer == null || pointer.isEmpty()) {
      return "JSON pointer is empty";
    }
    if (!pointer.startsWith("/")) {
      return "'" + pointer + "' is not a JSON pointer (must start with '/')";
    }
    for (int i = pointer.indexOf('~'); i >= 0; i = pointer.indexOf('~', i + 1)) {
      if (i + 1 >= pointer.length()
          || (pointer.charAt(i + 1) != '0' && pointer.charAt(i + 1) != '1')) {
        return "'" + pointer + "' is not a valid JSON pointer: '~' must be followed by '0' or '1'";
      }
    }
    try {
      JsonPointer.compile(pointer);
      return null;
    } catch (IllegalArgumentException e) {
      return "'" + pointer + "' is not a valid JSON pointer: " + e.getMessage();
    }
  }
}
