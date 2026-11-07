package io.github.matabar.kafkafim.route;

import java.util.List;

/**
 * One route of a pipeline, in the shape {@link Router#compile} takes.
 *
 * @param target what the caller wants back when the route is selected (e.g. the ES cluster,
 *     identity and ingest pipeline); opaque to {@code route}
 * @param when conditions that must all hold; empty for the default route, or for an unconditional
 *     route in {@link RouteMode#ALL}
 * @param isDefault {@code default: true}: taken only when no other route matched
 * @param index Logstash-style index pattern
 * @param <T> the caller's target type
 */
public record RouteSpec<T>(T target, List<ConditionSpec> when, boolean isDefault, String index) {

  /** Rejects missing components and copies {@code when}. */
  public RouteSpec {
    Specs.require(target != null, "route target is required");
    Specs.require(
        when != null && when.stream().noneMatch(java.util.Objects::isNull),
        "when must be a list of conditions");
    Specs.require(index != null, "route index is required");
    Specs.require(!isDefault || when.isEmpty(), "the default route must not have conditions");
    when = List.copyOf(when);
  }
}
