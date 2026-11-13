package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqOptions;
import io.github.matabar.kafkafim.sink.es.BreakerSettings;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import io.github.matabar.kafkafim.source.kafka.SourceOptions;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Status;

/** Pause routing and states, without starting any pipeline (no broker needed). */
class PipelineSupervisorTest {

  private final KafkafimProperties properties =
      TestConfigs.withPipelines(
          """
          to-es:
            source: { cluster: k, topics: [a] }
            routes: [ { default: true, cluster: es, index: a } ]
          to-both:
            source: { cluster: k, topics: [b] }
            route-mode: all
            routes:
              - { cluster: es, index: b }
              - { cluster: es2, index: b }
          to-es2:
            source: { cluster: k, topics: [c] }
            routes: [ { default: true, cluster: es2, index: c } ]
          """);

  static final PipelineResources.Settings SETTINGS =
      new PipelineResources.Settings(
          SourceOptions.defaults(),
          Duration.ofSeconds(1),
          DlqOptions.DEFAULTS,
          ClientSettings.DEFAULT,
          BreakerSettings.DEFAULT,
          Clock.systemUTC());

  private final List<String> events = new ArrayList<>();
  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final PipelineResources resources =
      new PipelineResources(relay, p -> (cluster, identity) -> Preflight.Status.OK, SETTINGS);
  private final PipelineSupervisor supervisor =
      new PipelineSupervisor(
          properties,
          resources,
          List.of(
              new SupervisorListener() {
                @Override
                public void onPauseReasonsChange(String pipeline, Set<String> reasons) {
                  events.add(pipeline + "=" + reasons);
                }
              }));

  {
    relay.attach(supervisor);
  }

  @AfterEach
  void close() {
    resources.close();
  }

  private PipelineRuntime runtime(String name) {
    return supervisor.runtimes().stream().filter(r -> r.name().equals(name)).findFirst().get();
  }

  /** The listener of the Elasticsearch registry that pipeline {@code to-es} sends through. */
  private SinkListener esListener() {
    Object owner = runtime("to-es").context().sinks().get("es");
    return relay.sinkListener(() -> owner);
  }

  /** The listener of the DLQ producers that pipeline {@code to-es} writes through. */
  private DlqListener dlqListener() {
    Object owner = runtime("to-es").context().dlqProducers();
    return relay.dlqListener(() -> owner);
  }

  @Test
  void breakerOpenPausesEveryPipelineRoutingToTheClusterUntilItCloses() {
    esListener().onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-both")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-es2")).isEmpty();

    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.HALF_OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");

    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.CLOSED);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    assertThat(events)
        .containsExactly(
            "to-es=[es-breaker:es]", "to-both=[es-breaker:es]", "to-es=[]", "to-both=[]");
  }

  @Test
  void forcedOpenPausesAndDisabledOrMetricsOnlyResumes() {
    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.FORCED_OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    esListener()
        .onBreakerStateChange(
            "es", CircuitBreaker.State.FORCED_OPEN, CircuitBreaker.State.DISABLED);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();

    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.DISABLED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.METRICS_ONLY);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
    // A repeated event changes nothing and reports nothing.
    esListener()
        .onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.METRICS_ONLY);
    assertThat(events)
        .containsExactly(
            "to-es=[es-breaker:es]",
            "to-both=[es-breaker:es]",
            "to-es=[]",
            "to-both=[]",
            "to-es=[es-breaker:es]",
            "to-both=[es-breaker:es]",
            "to-es=[]",
            "to-both=[]");
  }

  @Test
  void eachClustersBreakerPausesAndResumesOnItsOwn() {
    Object es2 = runtime("to-es2").context().sinks().get("es2");
    SinkListener es2Listener = relay.sinkListener(() -> es2);

    esListener().onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    es2Listener.onBreakerStateChange("es2", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-both"))
        .containsExactlyInAnyOrder("es-breaker:es", "es-breaker:es2");
    assertThat(supervisor.pauseReasons("to-es2")).containsExactly("es-breaker:es2");

    // es is back: a pipeline that also routes to es2 stays paused for es2.
    esListener().onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.CLOSED);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
    assertThat(supervisor.pauseReasons("to-both")).containsExactly("es-breaker:es2");
    assertThat(supervisor.pauseReasons("to-es2")).containsExactly("es-breaker:es2");

    es2Listener.onBreakerStateChange("es2", CircuitBreaker.State.OPEN, CircuitBreaker.State.CLOSED);
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    assertThat(supervisor.pauseReasons("to-es2")).isEmpty();
  }

  @Test
  void breakerEventsOfUnusedRegistriesAreIgnored() {
    // A breaker of another registry (a configuration generation every pipeline left), an event
    // without an owner, and es's registry reporting a cluster it does not own.
    relay
        .sinkListener(Object::new)
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    relay
        .sinkListener(() -> null)
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    esListener()
        .onBreakerStateChange("es2", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    assertThat(supervisor.pauseReasons("to-es2")).isEmpty();
    assertThat(events).isEmpty();

    // Nor does such an event resume what the pipelines' own registry paused.
    esListener().onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    relay
        .sinkListener(Object::new)
        .onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.CLOSED);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-both")).containsExactly("es-breaker:es");
  }

  /**
   * The wiring from a registry's own breaker to the pause, not just the supervisor's handler: the
   * registry {@link PipelineResources} built reports its breaker's transitions with itself as
   * owner. {@code breakerState} is what the breaker gauge shows.
   */
  @Test
  void theRegistrysOwnBreakerOpeningPausesItsPipelinesAndClosingResumesThem() {
    CircuitBreaker breaker = runtime("to-es").context().sinks().get("es").breaker("es");
    assertThat(supervisor.breakerState("es")).isEqualTo(CircuitBreaker.State.CLOSED);
    assertThat(supervisor.breakerState("nope")).isNull();

    breaker.transitionToOpenState();
    await().until(() -> supervisor.pauseReasons("to-es").equals(Set.of("es-breaker:es")));
    assertThat(supervisor.pauseReasons("to-both")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-es2")).isEmpty();
    assertThat(supervisor.breakerState("es")).isEqualTo(CircuitBreaker.State.OPEN);
    assertThat(supervisor.breakerState("es2")).isEqualTo(CircuitBreaker.State.CLOSED);

    breaker.transitionToHalfOpenState();
    await().until(() -> supervisor.breakerState("es") == CircuitBreaker.State.HALF_OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");

    breaker.transitionToClosedState();
    await().until(() -> supervisor.pauseReasons("to-es").isEmpty());
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    assertThat(supervisor.breakerState("es")).isEqualTo(CircuitBreaker.State.CLOSED);
  }

  @Test
  void authEventWithoutCurrentSinkLeavesNoPause() {
    // A late event from a sink of an earlier run (or of a pipeline that is not started) must not
    // pause a source that nothing would ever resume: the current sink's auth state decides.
    SinkKey key = new SinkKey("to-both", "es2", null);
    esListener().onAuthFailure(key, 401, "{}");
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    esListener().onAuthRecovered(key);
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
  }

  @Test
  void restartDropsBreakerReasonsButKeepsTheDlqBlockedReason() {
    esListener().onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    dlqListener().onBlocked("to-es", null);
    assertThat(supervisor.pauseReasons("to-es"))
        .containsExactlyInAnyOrder("dlq-blocked", "es-breaker:es");
    runtime("to-es").resetReasonsForStart();
    // The new run's senders probe the breaker again; the DLQ writer's retries clear dlq-blocked.
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("dlq-blocked");
  }

  @Test
  void blockedDlqPausesItsPipeline() {
    dlqListener().onBlocked("to-es", null);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("dlq-blocked");
    dlqListener().onUnblocked("to-es");
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
  }

  @Test
  void eventsForUnknownPipelinesAreIgnored() {
    dlqListener().onBlocked("gone", null);
    esListener().onAuthFailure(new SinkKey("gone", "es", null), 403, "{}");
    assertThat(supervisor.states()).containsOnlyKeys("to-es", "to-both", "to-es2");
  }

  @Test
  void pipelinesAreStoppedUntilStartedAndReadinessWaitsForOne() {
    assertThat(supervisor.states().values()).containsOnly(PipelineState.STOPPED);
    assertThat(new PipelinesHealthIndicator(supervisor).health().getStatus())
        .isEqualTo(Status.OUT_OF_SERVICE);
    assertThatThrownBy(() -> supervisor.state("nope")).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void noPipelinesIsReady() {
    KafkafimProperties empty = TestConfigs.withPipelines("{}");
    PipelineSupervisor idle = new PipelineSupervisor(empty, resources, List.of());
    assertThat(new PipelinesHealthIndicator(idle).health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void passthroughKeyTheConsumerOwnsFailsPreflightRatherThanRetrying() throws Exception {
    // The DLQ producer accepts max.poll.records; the consumer factory rejects it. That is a
    // configuration failure, not a group check that retries forever.
    KafkafimProperties broken =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k:
                    bootstrap-servers: ["127.0.0.1:9"]
                    properties: { max.poll.records: 5 }
              elasticsearch:
                clusters:
                  es: { uris: ["http://127.0.0.1:9"] }
              pipelines:
                bad:
                  source: { cluster: k, topics: [a] }
                  routes: [ { default: true, cluster: es, index: a } ]
            """);
    PipelineResources brokenResources =
        new PipelineResources(
            relay,
            p -> new ElasticsearchAuthCheck(p.elasticsearch().clusters()),
            new PipelineResources.Settings(
                // A 1s metadata timeout bounds the consumer group check against the dead broker.
                new SourceOptions(
                    1,
                    0.5,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1)),
                Duration.ofSeconds(1),
                new DlqOptions(
                    Duration.ofMillis(100),
                    Duration.ofSeconds(1),
                    Duration.ofMillis(500),
                    Duration.ofSeconds(1)),
                ClientSettings.DEFAULT,
                BreakerSettings.DEFAULT,
                Clock.systemUTC()));
    PipelineSupervisor failing = new PipelineSupervisor(broken, brokenResources, List.of());
    failing.startPipeline("bad");
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (failing.state("bad") == PipelineState.STARTING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertThat(failing.state("bad")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(failing.failure("bad"))
        .contains("configuration cannot run")
        .contains("max.poll.records");
    failing.stop();
    brokenResources.close();
  }

  @Test
  void failedPreflightIsReportedAndStopsNothingElse() throws Exception {
    // Kafka cluster k is unreachable, so the DLQ and group checks are transient; an unknown ES
    // identity is a configuration problem found in the same pass, which wins.
    KafkafimProperties broken =
        TestConfigs.bind(
            """
            kafkafim:
              kafka:
                clusters:
                  k: { bootstrap-servers: ["127.0.0.1:9"] }
              elasticsearch:
                clusters:
                  es: { uris: ["http://127.0.0.1:9"] }
              pipelines:
                bad:
                  source: { cluster: k, topics: [a] }
                  routes: [ { default: true, cluster: es, identity: ghost, index: a } ]
            """);
    PipelineResources brokenResources =
        new PipelineResources(
            relay,
            p -> new ElasticsearchAuthCheck(p.elasticsearch().clusters()),
            new PipelineResources.Settings(
                // A 1s metadata timeout bounds the consumer group check against the dead broker.
                new SourceOptions(
                    1,
                    0.5,
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1),
                    Duration.ofSeconds(1)),
                Duration.ofSeconds(1),
                new DlqOptions(
                    Duration.ofMillis(100),
                    Duration.ofSeconds(1),
                    Duration.ofMillis(500),
                    Duration.ofSeconds(1)),
                ClientSettings.DEFAULT,
                BreakerSettings.DEFAULT,
                Clock.systemUTC()));
    PipelineSupervisor failing = new PipelineSupervisor(broken, brokenResources, List.of());
    failing.startPipeline("bad");
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (failing.state("bad") == PipelineState.STARTING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertThat(failing.state("bad")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(failing.failure("bad")).contains("identity 'ghost'").contains("cluster 'es'");
    failing.stop();
    assertThat(failing.state("bad")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    brokenResources.close();
  }

  @Test
  void stopInterruptsPreflightChecksWaitingForServersThatNeverAnswer() throws Exception {
    // Both the broker and the Elasticsearch cluster accept connections and never answer, so the
    // DLQ describe and the _security/_authenticate request would each wait out a 10s timeout, and
    // the consumer group describe the source's metadata timeout (30s).
    try (ServerSocket silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress())) {
      List<Socket> accepted = new CopyOnWriteArrayList<>();
      Thread acceptor =
          Thread.ofVirtual()
              .start(() -> {
                try {
                  while (true) {
                    accepted.add(silent.accept());
                  }
                } catch (IOException e) {
                  // closed
                }
              });
      String address = "127.0.0.1:" + silent.getLocalPort();
      KafkafimProperties unanswered =
          TestConfigs.bind(
              """
              kafkafim:
                kafka:
                  clusters:
                    k: { bootstrap-servers: ["%1$s"] }
                elasticsearch:
                  clusters:
                    es: { uris: ["http://%1$s"] }
                pipelines:
                  slow:
                    source: { cluster: k, topics: [a] }
                    routes: [ { default: true, cluster: es, index: a } ]
              """
                  .formatted(address));
      PipelineResources slowResources =
          new PipelineResources(
              relay,
              p -> new ElasticsearchAuthCheck(p.elasticsearch().clusters()),
              new PipelineResources.Settings(
                  SourceOptions.defaults(),
                  Duration.ofSeconds(1),
                  new DlqOptions(
                      Duration.ofMillis(100),
                      Duration.ofSeconds(1),
                      Duration.ofSeconds(10),
                      Duration.ofMillis(200)),
                  ClientSettings.DEFAULT,
                  BreakerSettings.DEFAULT,
                  Clock.systemUTC()));
      PipelineSupervisor slow = new PipelineSupervisor(unanswered, slowResources, List.of());
      try {
        slow.startPipeline("slow");
        PipelineRuntime runtime = slow.runtimes().iterator().next();
        await().atMost(Duration.ofSeconds(10)).until(runtime::inPreflight);
        Thread.sleep(300); // well into the DLQ describe

        long began = System.nanoTime();
        slow.stop();
        Duration took = Duration.ofNanos(System.nanoTime() - began);

        assertThat(took).isLessThan(Duration.ofSeconds(5));
        assertThat(slow.state("slow")).isEqualTo(PipelineState.STOPPED);
        assertThat(slow.failure("slow")).isNull();
      } finally {
        // Hang up first: a producer closing against a silent broker waits out its request timeout.
        silent.close();
        acceptor.join();
        for (Socket socket : accepted) {
          socket.close();
        }
        slowResources.close();
      }
    }
  }
}
