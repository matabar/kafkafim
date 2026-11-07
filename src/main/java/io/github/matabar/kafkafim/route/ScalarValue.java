package io.github.matabar.kafkafim.route;

import java.math.BigDecimal;
import tools.jackson.databind.JsonNode;

/**
 * A configured {@code equals}/{@code in} value, pre-parsed once so that comparing it to a document
 * field allocates nothing for strings, booleans and integers in {@code long} range (other numbers
 * allocate one {@code BigDecimal} per comparison). Semantics: {@link ConditionSpec.FieldEquals}.
 */
final class ScalarValue {

  private final String text;
  private final BigDecimal decimal;
  private final boolean isLong;
  private final long longValue;

  private ScalarValue(String text) {
    this.text = text;
    BigDecimal parsed = null;
    try {
      parsed = new BigDecimal(text);
    } catch (NumberFormatException e) {
      // Not numeric: never equals a JSON number.
    }
    this.decimal = parsed;
    long exact = 0;
    boolean fits = false;
    if (parsed != null) {
      try {
        exact = parsed.longValueExact();
        fits = true;
      } catch (ArithmeticException e) {
        // Fractional or outside long: compared as BigDecimal.
      }
    }
    this.isLong = fits;
    this.longValue = exact;
  }

  static ScalarValue of(String text) {
    Specs.require(text != null, "value must not be null");
    return new ScalarValue(text);
  }

  boolean matches(JsonNode node) {
    if (node.isString()) {
      return text.equals(node.stringValue());
    }
    if (node.isNumber()) {
      return decimal != null && numberMatches(node);
    }
    if (node.isBoolean()) {
      return text.equals(node.booleanValue() ? "true" : "false");
    }
    return false;
  }

  private boolean numberMatches(JsonNode node) {
    if (node.isIntegralNumber() && node.canConvertToLong()) {
      return isLong && longValue == node.longValue();
    }
    BigDecimal value = decimalOf(node);
    return value != null && value.compareTo(decimal) == 0;
  }

  /** The node's value in its shortest decimal form, or {@code null} for NaN and infinities. */
  private static BigDecimal decimalOf(JsonNode node) {
    if (node.isDouble()) {
      double d = node.doubleValue();
      return Double.isFinite(d) ? BigDecimal.valueOf(d) : null;
    }
    if (node.isFloat()) {
      float f = node.floatValue();
      return Float.isFinite(f) ? new BigDecimal(Float.toString(f)) : null;
    }
    return node.decimalValue(null);
  }

  @Override
  public String toString() {
    return text;
  }
}
