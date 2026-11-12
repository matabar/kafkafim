package io.github.matabar.kafkafim.route;

import java.util.List;
import org.jspecify.annotations.Nullable;

/**
 * One route of a pipeline, in the shape {@link Router#compile} takes.
 *
 * @param target what the caller wants back when the route is selected (e.g. the ES cluster,
 *     identity and ingest pipeline, or only a label for a drop route); opaque to {@code route}
 * @param when conditions that must all hold; empty for the default route, or for an unconditional
 *     route in {@link RouteMode#ALL}
 * @param isDefault {@code default: true}: taken only when no other route matched
 * @param index Logstash-style index pattern; {@code null} exactly when {@code drop} is set
 * @param drop {@code drop: true}: a selected document is discarded instead of indexed
 * @param <T> the caller's target type
 */
public record RouteSpec<T>(
    T target, List<ConditionSpec> when, boolean isDefault, @Nullable String index, boolean drop) {

  /** Rejects missing components and copies {@code when}. */
  public RouteSpec {
    Specs.require(target != null, "route target is required");
    Specs.require(
        when != null && when.stream().noneMatch(java.util.Objects::isNull),
        "when must be a list of conditions");
    Specs.require(drop || index != null, "route index is required");
    Specs.require(!drop || index == null, "a drop route takes no index");
    Specs.require(!isDefault || when.isEmpty(), "the default route must not have conditions");
    when = List.copyOf(when);
  }

  /** An indexing route. */
  public RouteSpec(T target, List<ConditionSpec> when, boolean isDefault, String index) {
    this(target, when, isDefault, index, false);
  }

  /** A drop route: a document it selects goes nowhere (DESIGN §5.3). */
  public static <T> RouteSpec<T> drop(T target, List<ConditionSpec> when, boolean isDefault) {
    return new RouteSpec<>(target, when, isDefault, null, true);
  }
}
