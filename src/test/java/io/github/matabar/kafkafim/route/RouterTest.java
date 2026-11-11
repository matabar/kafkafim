package io.github.matabar.kafkafim.route;

import static io.github.matabar.kafkafim.route.ConditionTest.doc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.route.ConditionSpec.FieldEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldExists;
import io.github.matabar.kafkafim.route.ConditionSpec.FieldIn;
import io.github.matabar.kafkafim.route.ConditionSpec.HeaderEquals;
import io.github.matabar.kafkafim.route.ConditionSpec.TopicEquals;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Route selection in both modes, with and without the default route. */
class RouterTest {

  /** The DESIGN §5.2 example. */
  private static final List<RouteSpec<String>> EXAMPLE =
      List.of(
          new RouteSpec<>(
              "eu", List.of(new FieldEquals("/region", "eu")), false, "orders-eu-%{+yyyy.MM}"),
          new RouteSpec<>(
              "us",
              List.of(new FieldIn("/region", List.of("us", "ca")), new FieldExists("/tier", true)),
              false,
              "orders-%{region}-%{+yyyy.MM.dd}"),
          new RouteSpec<>("unrouted", List.of(), true, "orders-unrouted"));

  private static List<String> targets(Router<String> router, String json) {
    return targets(router, json, "orders.v2", HeaderLookup.NONE);
  }

  private static List<String> targets(
      Router<String> router, String json, String topic, HeaderLookup headers) {
    return router.select(doc(json), topic, headers).stream().map(Route::target).toList();
  }

  @Test
  void firstModeTakesTheFirstMatchInOrder() {
    Router<String> router = Router.compile(RouteMode.FIRST, EXAMPLE);
    assertThat(targets(router, "{\"region\":\"eu\",\"tier\":1}")).containsExactly("eu");
    assertThat(targets(router, "{\"region\":\"ca\",\"tier\":1}")).containsExactly("us");
  }

  @Test
  void everyConditionOfTheRouteMustHold() {
    Router<String> router = Router.compile(RouteMode.FIRST, EXAMPLE);
    assertThat(targets(router, "{\"region\":\"us\"}")).containsExactly("unrouted");
  }

  @Test
  void firstModeFallsBackToTheDefault() {
    Router<String> router = Router.compile(RouteMode.FIRST, EXAMPLE);
    assertThat(targets(router, "{\"region\":\"apac\"}")).containsExactly("unrouted");
    assertThat(targets(router, "{}")).containsExactly("unrouted");
  }

  @Test
  void firstModeStopsAtTheFirstOfSeveralMatches() {
    Router<String> router =
        Router.compile(
            RouteMode.FIRST,
            List.of(
                new RouteSpec<>("a", List.of(new FieldExists("/x", true)), false, "a"),
                new RouteSpec<>("b", List.of(new FieldExists("/x", true)), false, "b"),
                new RouteSpec<>("d", List.of(), true, "d")));
    assertThat(targets(router, "{\"x\":1}")).containsExactly("a");
  }

  @Test
  void allModeReturnsEveryMatchInOrderWithoutTheDefault() {
    Router<String> router =
        Router.compile(
            RouteMode.ALL,
            List.of(
                new RouteSpec<>("archive", List.of(), false, "archive"),
                new RouteSpec<>("eu", List.of(new FieldEquals("/region", "eu")), false, "eu"),
                new RouteSpec<>("audit", List.of(new TopicEquals("audit")), false, "audit"),
                new RouteSpec<>("d", List.of(), true, "d")));
    assertThat(targets(router, "{\"region\":\"eu\"}", "audit", HeaderLookup.NONE))
        .containsExactly("archive", "eu", "audit");
    assertThat(targets(router, "{\"region\":\"us\"}")).containsExactly("archive");
  }

  @Test
  void allModeTakesTheDefaultOnlyWhenNothingMatched() {
    Router<String> router =
        Router.compile(
            RouteMode.ALL,
            List.of(
                new RouteSpec<>("eu", List.of(new FieldEquals("/region", "eu")), false, "eu"),
                new RouteSpec<>("d", List.of(), true, "d")));
    assertThat(targets(router, "{\"region\":\"eu\"}")).containsExactly("eu");
    assertThat(targets(router, "{\"region\":\"us\"}")).containsExactly("d");
  }

  @Test
  void noMatchWithoutDefaultIsEmpty() {
    Router<String> router =
        Router.compile(
            RouteMode.ALL,
            List.of(new RouteSpec<>("eu", List.of(new FieldEquals("/region", "eu")), false, "eu")));
    assertThat(router.select(doc("{}"), "t", HeaderLookup.NONE)).isEmpty();
    assertThat(Router.NO_ROUTE).isEqualTo("no_route");
  }

  @Test
  void headersAndTopicsAreSubjects() {
    Router<String> router =
        Router.compile(
            RouteMode.FIRST,
            List.of(
                new RouteSpec<>("h", List.of(new HeaderEquals("tenant", "acme")), false, "h"),
                new RouteSpec<>("t", List.of(new TopicEquals("orders.backfill")), false, "t"),
                new RouteSpec<>("d", List.of(), true, "d")));
    HeaderLookup acme = HeaderLookup.of(Map.of("tenant", "acme".getBytes(StandardCharsets.UTF_8)));
    assertThat(targets(router, "{}", "orders.v2", acme)).containsExactly("h");
    assertThat(targets(router, "{}", "orders.backfill", HeaderLookup.NONE)).containsExactly("t");
    assertThat(targets(router, "{}", "orders.v2", HeaderLookup.NONE)).containsExactly("d");
  }

  @Test
  void firstModeLetsOrderDecideBetweenDropAndIndex() {
    Router<String> router =
        Router.compile(
            RouteMode.FIRST,
            List.of(
                new RouteSpec<>("keep", List.of(new FieldEquals("/level", "error")), false, "k"),
                RouteSpec.drop("debug", List.of(new FieldExists("/level", true)), false),
                new RouteSpec<>("d", List.of(), true, "d")));
    List<Route<String>> error = router.select(doc("{\"level\":\"error\"}"), "t", HeaderLookup.NONE);
    List<Route<String>> debug = router.select(doc("{\"level\":\"debug\"}"), "t", HeaderLookup.NONE);

    assertThat(error).extracting(Route::target).containsExactly("keep");
    assertThat(error.getFirst().isDrop()).isFalse();
    assertThat(debug).extracting(Route::target).containsExactly("debug");
    assertThat(debug.getFirst().isDrop()).isTrue();
    assertThat(targets(router, "{}")).containsExactly("d");
  }

  @Test
  void allModeDropCancelsEveryOtherCopy() {
    Router<String> router =
        Router.compile(
            RouteMode.ALL,
            List.of(
                new RouteSpec<>("archive", List.of(), false, "archive"),
                RouteSpec.drop("test-traffic", List.of(new HeaderEquals("test", "1")), false),
                new RouteSpec<>("eu", List.of(new FieldEquals("/region", "eu")), false, "eu"),
                RouteSpec.drop("eu-debug", List.of(new FieldEquals("/region", "eu")), false)));
    HeaderLookup test = HeaderLookup.of(Map.of("test", "1".getBytes(StandardCharsets.UTF_8)));

    assertThat(targets(router, "{\"region\":\"eu\"}")).containsExactly("eu-debug");
    assertThat(targets(router, "{\"region\":\"eu\"}", "t", test))
        .as("the first matching drop route labels the document")
        .containsExactly("test-traffic");
    assertThat(targets(router, "{\"region\":\"us\"}")).containsExactly("archive");
  }

  @Test
  void dropDefaultTurnsTheRoutesIntoAnAllowlist() {
    for (RouteMode mode : RouteMode.values()) {
      Router<String> router =
          Router.compile(
              mode,
              List.of(
                  new RouteSpec<>("orders", List.of(new FieldEquals("/type", "order")), false, "o"),
                  RouteSpec.drop("rest", List.of(), true)));
      List<Route<String>> other = router.select(doc("{\"type\":\"ping\"}"), "t", HeaderLookup.NONE);

      assertThat(targets(router, "{\"type\":\"order\"}")).as(mode.name()).containsExactly("orders");
      assertThat(other).extracting(Route::target).as(mode.name()).containsExactly("rest");
      assertThat(other.getFirst().isDrop()).isTrue();
      assertThat(other.getFirst().isDefault()).isTrue();
    }
  }

  @Test
  void dropRouteHasNoIndex() {
    Route<String> drop =
        Router.compile(RouteMode.FIRST, List.of(RouteSpec.drop("x", List.of(), true)))
            .routes()
            .getFirst();
    assertThat(drop.isDrop()).isTrue();
    assertThat(drop).hasToString("Route[0, default, drop]");
    assertThatThrownBy(drop::index).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> new RouteSpec<>("x", List.of(), true, "idx", true))
        .hasMessage("a drop route takes no index");
  }

  @Test
  void exposesCompiledRoutes() {
    Router<String> router = Router.compile(RouteMode.FIRST, EXAMPLE);
    assertThat(router.mode()).isEqualTo(RouteMode.FIRST);
    assertThat(router.routes()).extracting(Route::position).containsExactly(0, 1, 2);
    assertThat(router.routes()).extracting(Route::isDefault).containsExactly(false, false, true);
    assertThat(router.routes().get(1).index().pattern())
        .isEqualTo("orders-%{region}-%{+yyyy.MM.dd}");
  }

  @Test
  void selectionResultsAreUnmodifiable() {
    Router<String> router =
        Router.compile(
            RouteMode.ALL,
            List.of(
                new RouteSpec<>("a", List.of(), false, "a"),
                new RouteSpec<>("b", List.of(), false, "b")));
    List<Route<String>> selected = router.select(doc("{}"), "t", HeaderLookup.NONE);
    assertThat(selected).hasSize(2);
    assertThatThrownBy(selected::clear).isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rejectsGarbage() {
    assertThatThrownBy(() -> Router.compile(RouteMode.FIRST, List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Router.<String>compile(null, EXAMPLE))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            Router.compile(
                RouteMode.ALL,
                List.of(
                    new RouteSpec<>("a", List.of(), true, "a"),
                    new RouteSpec<>("b", List.of(), true, "b"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("at most one default");
    assertThatThrownBy(
            () -> new RouteSpec<>("d", List.<ConditionSpec>of(new TopicEquals("t")), true, "d"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() ->
            Router.compile(
                RouteMode.FIRST, List.of(new RouteSpec<>("a", List.of(), true, "bad%{"))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("route 0: unterminated");
  }
}
