package io.github.matabar.kafkafim.route;

/**
 * Elasticsearch's index-name rules, applied to a resolved (already lower-cased) name. The forbidden
 * characters and leading characters are the same set the configuration check applies to literal
 * text (DESIGN §5.7).
 */
final class IndexNames {

  /** Characters Elasticsearch rejects in index names. */
  static final String FORBIDDEN_CHARS = "\\/*?\"<>|,#: ";

  /** Elasticsearch's limit, in UTF-8 bytes. */
  static final int MAX_BYTES = 255;

  private IndexNames() {}

  /** Which rule {@code name} breaks, or {@code null} if Elasticsearch accepts it. */
  static String problem(String name) {
    if (name.isEmpty()) {
      return "must not be empty";
    }
    if (name.equals(".") || name.equals("..")) {
      return "must not be '.' or '..'";
    }
    char first = name.charAt(0);
    if (first == '-' || first == '_' || first == '+') {
      return "must not start with '" + first + "'";
    }
    int bytes = 0;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      if (FORBIDDEN_CHARS.indexOf(c) >= 0) {
        return "must not contain '" + c + "'";
      }
      if (c < 0x80) {
        bytes += 1;
      } else if (c < 0x800) {
        bytes += 2;
      } else if (Character.isHighSurrogate(c)
          && i + 1 < name.length()
          && Character.isLowSurrogate(name.charAt(i + 1))) {
        bytes += 4;
        i++;
      } else {
        bytes += 3;
      }
    }
    if (bytes > MAX_BYTES) {
      return "is " + bytes + " UTF-8 bytes, more than " + MAX_BYTES;
    }
    return null;
  }
}
