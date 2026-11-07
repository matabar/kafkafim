package io.github.matabar.kafkafim.route;

/** How many matching routes a document is sent to (DESIGN §5.3). */
public enum RouteMode {
  /** The first matching route in order; the default route when none matched. */
  FIRST,
  /** Every matching route in order (fan-out); the default route only when none matched. */
  ALL
}
