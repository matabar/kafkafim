package io.github.matabar.kafkafim.route;

import java.util.List;
import org.jspecify.annotations.Nullable;
import tools.jackson.databind.JsonNode;

/**
 * A compiled route: its conditions, its {@link IndexPattern} (none for a drop route) and the
 * caller's target. Immutable and thread-safe.
 *
 * @param <T> the caller's target type
 */
public final class Route<T> {

  private final T target;
  private final Condition[] conditions;
  private final boolean isDefault;
  private final @Nullable IndexPattern index;
  private final int position;

  /**
   * {@code List.of(this)}, built once so that {@link RouteMode#FIRST} selection allocates nothing.
   */
  private final List<Route<T>> alone;

  Route(RouteSpec<T> spec, int position) {
    this.target = spec.target();
    this.conditions = spec.when().stream().map(Condition::compile).toArray(Condition[]::new);
    this.isDefault = spec.isDefault();
    this.index = spec.drop() ? null : IndexPattern.compile(spec.index());
    this.position = position;
    this.alone = List.of(this);
  }

  boolean matches(JsonNode document, String topic, HeaderLookup headers) {
    for (Condition condition : conditions) {
      if (!condition.test(document, topic, headers)) {
        return false;
      }
    }
    return true;
  }

  List<Route<T>> alone() {
    return alone;
  }

  /** The target given in the {@link RouteSpec}. */
  public T target() {
    return target;
  }

  /**
   * The compiled index pattern; resolve it per document with {@link IndexPattern#resolve}.
   *
   * @throws IllegalStateException for a drop route, which has none
   */
  public IndexPattern index() {
    if (index == null) {
      throw new IllegalStateException(this + " drops documents and has no index");
    }
    return index;
  }

  /** Whether a document this route selects is discarded instead of indexed (DESIGN §5.3). */
  public boolean isDrop() {
    return index == null;
  }

  /** Whether this is the fallback route. */
  public boolean isDefault() {
    return isDefault;
  }

  /** Zero-based position in the pipeline's route list, the metric label for unnamed routes. */
  public int position() {
    return position;
  }

  @Override
  public String toString() {
    return "Route["
        + position
        + (isDefault ? ", default" : "")
        + (index == null ? ", drop" : ", index=" + index)
        + "]";
  }
}
