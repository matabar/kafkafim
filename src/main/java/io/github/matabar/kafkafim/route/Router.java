package io.github.matabar.kafkafim.route;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import tools.jackson.databind.JsonNode;

/**
 * Selects the routes of one pipeline for a document (DESIGN §5.3). Built once per pipeline (and
 * again on reload), then shared by every consumer thread: immutable and thread-safe.
 *
 * <ul>
 *   <li>{@link RouteMode#FIRST}: the first non-default route, in order, whose conditions all hold;
 *       else the default route.
 *   <li>{@link RouteMode#ALL}: every non-default route whose conditions all hold, in order; the
 *       default route only when none did.
 * </ul>
 *
 * <p>An empty result means no route matched and there is no default: the caller sends the document
 * to the DLQ with {@code error.type=}{@value #NO_ROUTE}.
 *
 * @param <T> the caller's target type
 */
public final class Router<T> {

  /** DLQ {@code error.type} for an empty {@link #select} result. */
  public static final String NO_ROUTE = "no_route";

  private final RouteMode mode;
  private final List<Route<T>> routes;
  private final Route<T>[] conditional;
  private final List<Route<T>> fallback;

  private Router(RouteMode mode, List<Route<T>> routes) {
    this.mode = mode;
    this.routes = List.copyOf(routes);
    @SuppressWarnings("unchecked")
    Route<T>[] nonDefault = routes.stream().filter(r -> !r.isDefault()).toArray(Route[]::new);
    this.conditional = nonDefault;
    this.fallback =
        routes.stream().filter(Route::isDefault).findFirst().map(Route::alone).orElse(List.of());
  }

  /**
   * Compiles a pipeline's routes. Assumes the configuration was validated; still rejects input that
   * cannot be evaluated.
   *
   * @param mode {@code route-mode}
   * @param specs the routes in configuration order
   * @param <T> the caller's target type
   * @return the router
   * @throws IllegalArgumentException if {@code specs} is empty, has more than one default route, or
   *     a condition or index pattern does not compile
   */
  public static <T> Router<T> compile(RouteMode mode, List<RouteSpec<T>> specs) {
    Specs.require(mode != null, "route mode is required");
    Specs.require(specs != null && !specs.isEmpty(), "at least one route is required");
    List<Route<T>> routes = new ArrayList<>(specs.size());
    int defaults = 0;
    for (int i = 0; i < specs.size(); i++) {
      RouteSpec<T> spec = specs.get(i);
      Specs.require(spec != null, "route " + i + " is null");
      if (spec.isDefault()) {
        defaults++;
      }
      try {
        routes.add(new Route<>(spec, i));
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("route " + i + ": " + e.getMessage(), e);
      }
    }
    Specs.require(defaults <= 1, "at most one default route is allowed, found " + defaults);
    return new Router<>(mode, routes);
  }

  /**
   * The routes this document goes to, in configuration order. Never throws for document content.
   *
   * @param document the decoded document
   * @param topic the source topic of the record
   * @param headers the record's headers
   * @return the selected routes; unmodifiable, empty when nothing matched and there is no default
   */
  public List<Route<T>> select(JsonNode document, String topic, HeaderLookup headers) {
    Objects.requireNonNull(document, "document");
    Objects.requireNonNull(headers, "headers");
    if (mode == RouteMode.FIRST) {
      for (Route<T> route : conditional) {
        if (route.matches(document, topic, headers)) {
          return route.alone();
        }
      }
      return fallback;
    }
    Route<T> first = null;
    List<Route<T>> all = null;
    for (Route<T> route : conditional) {
      if (route.matches(document, topic, headers)) {
        if (first == null) {
          first = route;
        } else {
          if (all == null) {
            all = new ArrayList<>(conditional.length);
            all.add(first);
          }
          all.add(route);
        }
      }
    }
    if (all != null) {
      return Collections.unmodifiableList(all);
    }
    return first != null ? first.alone() : fallback;
  }

  /** {@code route-mode}. */
  public RouteMode mode() {
    return mode;
  }

  /** Every route, default included, in configuration order. */
  public List<Route<T>> routes() {
    return routes;
  }
}
