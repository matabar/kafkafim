package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.dlq.DlqOptions;
import io.github.matabar.kafkafim.dlq.DlqProducers;
import io.github.matabar.kafkafim.sink.es.BreakerSettings;
import io.github.matabar.kafkafim.sink.es.ClientSettings;
import io.github.matabar.kafkafim.sink.es.ElasticsearchSinks;
import io.github.matabar.kafkafim.sink.es.SinkKey;
import io.github.matabar.kafkafim.source.kafka.ConsumerFactories;
import io.github.matabar.kafkafim.source.kafka.SourceOptions;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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

  private final List<String> events = new ArrayList<>();
  private final ListenerRelay relay = new ListenerRelay(List.of(), List.of());
  private final DlqProducers dlq = new DlqProducers(properties, DlqListener.NONE);
  private final ElasticsearchSinks sinks =
      new ElasticsearchSinks(
          properties.elasticsearch().clusters(),
          ClientSettings.DEFAULT,
          BreakerSettings.DEFAULT,
          relay.sinkListener());
  private final PipelineSupervisor supervisor =
      new PipelineSupervisor(
          new PipelineRuntime.Context(
              properties,
              new ConsumerFactories(properties),
              dlq,
              sinks,
              (cluster, identity) -> Preflight.Status.OK,
              SourceOptions.defaults(),
              Duration.ofSeconds(1),
              Clock.systemUTC()),
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
    sinks.close();
    dlq.close();
  }

  @Test
  void breakerOpenPausesEveryPipelineRoutingToTheClusterUntilItCloses() {
    relay
        .sinkListener()
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-both")).containsExactly("es-breaker:es");
    assertThat(supervisor.pauseReasons("to-es2")).isEmpty();

    relay
        .sinkListener()
        .onBreakerStateChange("es", CircuitBreaker.State.OPEN, CircuitBreaker.State.HALF_OPEN);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("es-breaker:es");

    relay
        .sinkListener()
        .onBreakerStateChange("es", CircuitBreaker.State.HALF_OPEN, CircuitBreaker.State.CLOSED);
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    assertThat(events)
        .containsExactly(
            "to-es=[es-breaker:es]", "to-both=[es-breaker:es]", "to-es=[]", "to-both=[]");
  }

  @Test
  void authEventWithoutCurrentSinkLeavesNoPause() {
    // A late event from a sink of an earlier run (or of a pipeline that is not started) must not
    // pause a source that nothing would ever resume: the current sink's auth state decides.
    SinkKey key = new SinkKey("to-both", "es2", null);
    relay.sinkListener().onAuthFailure(key, 401, "{}");
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
    relay.sinkListener().onAuthRecovered(key);
    assertThat(supervisor.pauseReasons("to-both")).isEmpty();
  }

  @Test
  void restartDropsBreakerReasonsButKeepsTheDlqBlockedReason() {
    relay
        .sinkListener()
        .onBreakerStateChange("es", CircuitBreaker.State.CLOSED, CircuitBreaker.State.OPEN);
    relay.dlqListener().onBlocked("to-es", null);
    assertThat(supervisor.pauseReasons("to-es"))
        .containsExactlyInAnyOrder("dlq-blocked", "es-breaker:es");
    PipelineRuntime toEs =
        supervisor.runtimes().stream().filter(r -> r.name().equals("to-es")).findFirst().get();
    toEs.resetReasonsForStart();
    // The new run's senders probe the breaker again; the DLQ writer's retries clear dlq-blocked.
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("dlq-blocked");
  }

  @Test
  void blockedDlqPausesItsPipeline() {
    relay.dlqListener().onBlocked("to-es", null);
    assertThat(supervisor.pauseReasons("to-es")).containsExactly("dlq-blocked");
    relay.dlqListener().onUnblocked("to-es");
    assertThat(supervisor.pauseReasons("to-es")).isEmpty();
  }

  @Test
  void eventsForUnknownPipelinesAreIgnored() {
    relay.dlqListener().onBlocked("gone", null);
    relay.sinkListener().onAuthFailure(new SinkKey("gone", "es", null), 403, "{}");
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
    PipelineSupervisor idle =
        new PipelineSupervisor(
            new PipelineRuntime.Context(
                empty,
                new ConsumerFactories(empty),
                dlq,
                sinks,
                (cluster, identity) -> Preflight.Status.OK,
                SourceOptions.defaults(),
                Duration.ofSeconds(1),
                Clock.systemUTC()),
            List.of());
    assertThat(new PipelinesHealthIndicator(idle).health().getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void failedPreflightIsReportedAndStopsNothingElse() throws Exception {
    // Kafka cluster k is unreachable, so the DLQ check is transient; an unknown ES identity is a
    // configuration problem found in the same pass, which wins.
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
    DlqProducers brokenDlq =
        new DlqProducers(
            broken,
            DlqListener.NONE,
            new DlqOptions(
                Duration.ofMillis(100),
                Duration.ofSeconds(1),
                Duration.ofMillis(500),
                Duration.ofSeconds(1)));
    PipelineSupervisor failing =
        new PipelineSupervisor(
            new PipelineRuntime.Context(
                broken,
                new ConsumerFactories(broken),
                brokenDlq,
                sinks,
                new ElasticsearchAuthCheck(broken.elasticsearch().clusters()),
                SourceOptions.defaults(),
                Duration.ofSeconds(1),
                Clock.systemUTC()),
            List.of());
    failing.startPipeline("bad");
    long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
    while (failing.state("bad") == PipelineState.STARTING && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertThat(failing.state("bad")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    assertThat(failing.failure("bad")).contains("identity 'ghost'").contains("cluster 'es'");
    failing.stop();
    assertThat(failing.state("bad")).isEqualTo(PipelineState.FAILED_PREFLIGHT);
    brokenDlq.close();
  }
}
