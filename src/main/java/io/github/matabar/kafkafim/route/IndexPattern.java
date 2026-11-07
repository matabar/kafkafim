package io.github.matabar.kafkafim.route;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import tools.jackson.core.JsonPointer;
import tools.jackson.databind.JsonNode;

/**
 * A compiled Logstash-style index pattern (DESIGN §5.3): literal text, {@code %{field}} and {@code
 * %{+DateTimeFormatter}}, parsed exactly as the configuration check parses it: a reference starts
 * at {@code %{} and ends at the next {@code }}; everything else, including a lone {@code %} or
 * {@code }}, is literal (there is no escape syntax).
 *
 * <ul>
 *   <li>{@code %{/a/b}} is a JSON pointer, {@code %{name}} the top-level property {@code name}
 *       (taken literally, so {@code %{a.b}} is the property {@code "a.b"}).
 *   <li>A field that is a string contributes its text; a number or boolean Jackson's text form
 *       ({@code 42}, {@code 1.5}, {@code true}). A missing field, JSON {@code null}, an object or
 *       an array is {@link IndexResolution.Unresolved}.
 *   <li>{@code %{+pattern}} formats the event {@link Instant} in UTC with {@link Locale#ROOT}.
 *   <li>The whole result is lower-cased with {@link Locale#ROOT} and checked against
 *       Elasticsearch's index-name rules; a violation is {@link IndexResolution.InvalidName}.
 * </ul>
 *
 * <p>Instances are immutable and thread-safe. A pattern without references is resolved once, at
 * compile time.
 */
public final class IndexPattern {

  private final String pattern;
  private final Part[] parts;
  private final boolean usesEventTime;
  private final int sizeHint;

  /** Pre-resolved result for a pattern without references, else {@code null}. */
  private final IndexResolution constant;

  private IndexPattern(String pattern, List<Part> parts) {
    this.pattern = pattern;
    this.parts = parts.toArray(Part[]::new);
    this.usesEventTime = parts.stream().anyMatch(p -> p instanceof DateRef);
    this.sizeHint = Math.max(16, pattern.length() + 16);
    this.constant =
        parts.stream().allMatch(p -> p instanceof Literal) ? validated(literalText(parts)) : null;
    if (constant instanceof IndexResolution.InvalidName invalid) {
      throw new IllegalArgumentException(invalid.reason());
    }
  }

  /**
   * Parses {@code pattern}.
   *
   * @param pattern a pattern that passed configuration validation
   * @return the compiled pattern
   * @throws IllegalArgumentException if it is empty, has an unterminated or empty <code>%{</code>,
   *     an invalid JSON pointer or field name, or a date pattern {@link DateTimeFormatter} rejects
   */
  public static IndexPattern compile(String pattern) {
    Specs.require(pattern != null && !pattern.isBlank(), "index pattern is empty");
    List<Part> parts = new ArrayList<>();
    int position = 0;
    while (position < pattern.length()) {
      int open = pattern.indexOf("%{", position);
      int literalEnd = open < 0 ? pattern.length() : open;
      if (literalEnd > position) {
        parts.add(new Literal(pattern.substring(position, literalEnd)));
      }
      if (open < 0) {
        break;
      }
      int close = pattern.indexOf('}', open + 2);
      Specs.require(close >= 0, "unterminated '%{' at position " + open + " in '" + pattern + "'");
      parts.add(reference(pattern.substring(open + 2, close)));
      position = close + 1;
    }
    return new IndexPattern(pattern, parts);
  }

  private static Part reference(String reference) {
    Specs.require(!reference.isEmpty(), "empty reference '%{}'");
    if (reference.startsWith("+")) {
      String format = reference.substring(1);
      Specs.require(!format.isBlank(), "empty date pattern '%{+}'");
      DateTimeFormatter formatter =
          DateTimeFormatter.ofPattern(format, Locale.ROOT).withZone(ZoneOffset.UTC);
      return new DateRef(reference, formatter);
    }
    if (reference.startsWith("/")) {
      return new FieldRef(reference, Specs.pointer(reference), null);
    }
    Specs.require(
        reference.chars().noneMatch(Character::isWhitespace),
        "invalid field name '%{" + reference + "}'");
    return new FieldRef(reference, null, reference);
  }

  /**
   * Resolves the pattern for one document. Never throws for document content.
   *
   * @param document the decoded document
   * @param eventTime the document's event time (or ingest time); only read, and then required, when
   *     the pattern has a {@code %{+...}} reference
   * @return the index name, or why there is none
   */
  public IndexResolution resolve(JsonNode document, Instant eventTime) {
    if (constant != null) {
      return constant;
    }
    Objects.requireNonNull(document, "document");
    if (usesEventTime) {
      Objects.requireNonNull(eventTime, "eventTime");
    }
    StringBuilder name = new StringBuilder(sizeHint);
    for (Part part : parts) {
      switch (part) {
        case Literal literal -> name.append(literal.text());
        case FieldRef field -> {
          JsonNode node = field.lookup(document);
          String problem = appendScalar(node, name);
          if (problem != null) {
            return new IndexResolution.Unresolved(field.reference(), problem);
          }
        }
        case DateRef date -> {
          try {
            date.formatter().formatTo(eventTime, name);
          } catch (DateTimeException e) {
            return new IndexResolution.Unresolved(
                date.reference(), "not formattable for " + eventTime + ": " + e.getMessage());
          }
        }
      }
    }
    return validated(name.toString());
  }

  /** Appends a scalar's text; returns why the node is unusable, or {@code null}. */
  private static String appendScalar(JsonNode node, StringBuilder name) {
    if (node == null || node.isMissingNode()) {
      return "missing";
    }
    if (node.isString()) {
      name.append(node.stringValue());
      return null;
    }
    if (node.isNumber() || node.isBoolean()) {
      name.append(node.asString());
      return null;
    }
    if (node.isNull()) {
      return "null";
    }
    if (node.isObject()) {
      return "an object";
    }
    if (node.isArray()) {
      return "an array";
    }
    return "not a string, number or boolean";
  }

  private static IndexResolution validated(String raw) {
    String index = raw.toLowerCase(Locale.ROOT);
    String problem = IndexNames.problem(index);
    return problem == null
        ? new IndexResolution.Resolved(index)
        : new IndexResolution.InvalidName(index, problem);
  }

  private static String literalText(List<Part> parts) {
    StringBuilder text = new StringBuilder();
    for (Part part : parts) {
      text.append(((Literal) part).text());
    }
    return text.toString();
  }

  /** Whether resolving needs the event time ({@code %{+...}} present). */
  public boolean usesEventTime() {
    return usesEventTime;
  }

  /** The pattern as configured. */
  public String pattern() {
    return pattern;
  }

  @Override
  public String toString() {
    return pattern;
  }

  private sealed interface Part {}

  private record Literal(String text) implements Part {}

  /** Exactly one of {@code pointer} and {@code name} is set. */
  private record FieldRef(String reference, JsonPointer pointer, String name) implements Part {
    JsonNode lookup(JsonNode document) {
      return pointer != null ? document.at(pointer) : document.get(name);
    }
  }

  private record DateRef(String reference, DateTimeFormatter formatter) implements Part {}
}
