package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.dlq.TopicStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;

/**
 * The checks a pipeline passes before its source starts (DESIGN §4.6, ADR-0021): the DLQ topic
 * exists and its writer identity may describe it, and every {@code (cluster, identity)} its routes
 * use authenticates against Elasticsearch. The source topics are checked by the source itself when
 * it starts ({@code MissingTopicsException}).
 *
 * <p>Every check runs, so one log line names every problem at once. A <b>configuration</b> problem
 * (missing topic, rejected identity) keeps the pipeline in {@code failed_preflight}; a
 * <b>transient</b> one (broker or cluster unreachable) is retried with backoff, since a pod
 * starting during a short outage should not need a restart.
 */
final class Preflight {

  private Preflight() {}

  /** Asks Elasticsearch whether an identity authenticates. */
  @FunctionalInterface
  interface AuthCheck {

    /**
     * Checks one identity.
     *
     * @param cluster Elasticsearch cluster name
     * @param identity identity name, or {@code null} for none
     * @return the verdict
     */
    Status check(String cluster, @Nullable String identity);
  }

  /** What one check found. */
  record Status(Kind kind, @Nullable String detail) {

    static final Status OK = new Status(Kind.OK, null);

    static Status rejected(@Nullable String detail) {
      return new Status(Kind.CONFIGURATION, detail);
    }

    static Status unreachable(@Nullable String detail) {
      return new Status(Kind.TRANSIENT, detail);
    }
  }

  /** Check verdicts. */
  enum Kind {
    /** Passed. */
    OK,
    /** The resource is missing or the identity is rejected: the pipeline cannot run. */
    CONFIGURATION,
    /** No answer: try again later. */
    TRANSIENT
  }

  /**
   * The outcome of all checks.
   *
   * @param configuration configuration problems, each naming the resource and identity
   * @param transientProblems transient problems
   */
  record Result(List<String> configuration, List<String> transientProblems) {

    Result {
      configuration = List.copyOf(configuration);
      transientProblems = List.copyOf(transientProblems);
    }

    boolean passed() {
      return configuration.isEmpty() && transientProblems.isEmpty();
    }
  }

  /**
   * Runs the DLQ and Elasticsearch checks of one pipeline.
   *
   * @param compiled the compiled pipeline
   * @param dlqTopic describes the pipeline's DLQ topic with its DLQ identity
   * @param auth the Elasticsearch identity check
   * @return every problem found
   */
  static Result run(CompiledPipeline compiled, Supplier<TopicStatus> dlqTopic, AuthCheck auth) {
    List<String> configuration = new ArrayList<>();
    List<String> transientProblems = new ArrayList<>();
    Pipeline config = compiled.config();
    String dlq =
        "DLQ topic '"
            + config.dlq().topic()
            + "' (identity '"
            + Objects.toString(config.dlq().identity(), "<none>")
            + "' on Kafka cluster '"
            + config.source().cluster()
            + "')";
    TopicStatus topic;
    try {
      topic = dlqTopic.get();
    } catch (RuntimeException e) {
      topic = TopicStatus.UNREACHABLE;
    }
    if (topic == TopicStatus.MISSING) {
      configuration.add(dlq + " does not exist");
    } else if (topic == TopicStatus.NOT_AUTHORIZED) {
      configuration.add(dlq + ": identity may not describe it");
    } else if (topic != TopicStatus.EXISTS) {
      transientProblems.add(dlq + ": broker did not answer");
    }
    for (RouteTarget.StreamKey stream : compiled.streams().keySet()) {
      Status status;
      try {
        status = auth.check(stream.cluster(), stream.identity());
      } catch (RuntimeException e) {
        status = Status.unreachable(e.toString());
      }
      String what =
          "Elasticsearch identity '"
              + Objects.toString(stream.identity(), "<none>")
              + "' on cluster '"
              + stream.cluster()
              + "'";
      if (status.kind() == Kind.CONFIGURATION) {
        configuration.add(what + " rejected: " + status.detail());
      } else if (status.kind() == Kind.TRANSIENT) {
        transientProblems.add(what + " not reachable: " + status.detail());
      }
    }
    return new Result(configuration, transientProblems);
  }
}
