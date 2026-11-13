package io.github.matabar.kafkafim.supervisor;

import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.jspecify.annotations.Nullable;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * The preflight check of a pipeline's consumer group (DESIGN §4.6, ADR-0038): one {@code
 * DescribeGroups} with the source identity, asking for the identity's authorized operations.
 *
 * <p>A denied join is otherwise invisible: the listener container retries it every {@code
 * authRetryInterval} and the pipeline shows {@code running}. What kafka-clients 4.2 reports, by the
 * identity's group ACLs:
 *
 * <ul>
 *   <li>none (or a wrong group name): {@code GroupAuthorizationException} from {@code
 *       FindCoordinator}, which needs {@code Describe}; rejected;
 *   <li>{@code Describe} only, on a group that exists: authorized operations without {@code READ};
 *       rejected;
 *   <li>{@code Read} (which implies {@code Describe}), group exists: {@code READ} listed; passes;
 *   <li>{@code Describe} or {@code Read}, group never joined: {@code GroupIdNotFoundException}, so
 *       {@code Read} cannot be told from {@code Describe}; passes, and the first join remains the
 *       check for that one case.
 * </ul>
 *
 * <p>A broker without an authorizer lists every operation. A timeout or connection error is
 * transient.
 *
 * <p>The same admin client asks, in parallel, for the identity's authorized operations on the
 * source topics (ADR-0039): the source's own start check is a metadata fetch, which {@code
 * Describe} alone satisfies, so a missing {@code Read} would leave the pipeline {@code running}
 * with every fetch denied. A topic whose operations are listed without {@code READ} is rejected.
 * Everything else about a topic passes here, because the source's start reports it: a topic that
 * does not exist or is not visible, and a broker that does not answer.
 */
final class KafkaGroupCheck {

  private KafkaGroupCheck() {}

  /**
   * Checks that the source identity may read its consumer group.
   *
   * @param factory the consumer factory of the pipeline's {@code (cluster, identity)}
   * @param groupId the pipeline's consumer group
   * @param clientId client id of the short-lived admin client
   * @param timeout bound on the request
   * @return the verdict
   */
  static Preflight.Status check(
      ConsumerFactory<?, ?> factory, String groupId, String clientId, Duration timeout) {
    return check(factory, groupId, List.of(), clientId, timeout).group();
  }

  /**
   * Checks that the source identity may read its consumer group and its topics, with one admin
   * client and both requests in flight together, so the topics add no waiting time.
   *
   * @param factory the consumer factory of the pipeline's {@code (cluster, identity)}
   * @param groupId the pipeline's consumer group
   * @param topics the pipeline's source topics
   * @param clientId client id of the short-lived admin client
   * @param timeout bound on both requests together
   * @return the verdict for the group, and one per topic
   */
  static Preflight.SourceAccess check(
      ConsumerFactory<?, ?> factory,
      String groupId,
      List<String> topics,
      String clientId,
      Duration timeout) {
    Map<String, Object> settings = adminSettings(factory.getConfigurationProperties(), clientId);
    settings.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
    settings.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis());
    Admin admin;
    try {
      admin = Admin.create(settings);
    } catch (RuntimeException e) {
      // Bad settings, or no resolvable bootstrap host; the DLQ producer on the same cluster is
      // built first and already fails the same way.
      return new Preflight.SourceAccess(
          Preflight.Status.rejected(
              "cannot create a Kafka admin client: "
                  + e
                  + (e.getCause() == null ? "" : " (" + e.getCause() + ")")),
          Map.of());
    }
    try {
      long deadline = System.nanoTime() + timeout.toNanos();
      KafkaFuture<ConsumerGroupDescription> group =
          admin
              .describeConsumerGroups(
                  List.of(groupId),
                  new DescribeConsumerGroupsOptions().includeAuthorizedOperations(true))
              .describedGroups()
              .get(groupId);
      Map<String, KafkaFuture<TopicDescription>> described =
          topics.isEmpty()
              ? Map.of()
              : admin
                  .describeTopics(
                      new LinkedHashSet<>(topics),
                      new DescribeTopicsOptions().includeAuthorizedOperations(true))
                  .topicNameValues();
      Preflight.Status groupStatus;
      try {
        groupStatus = classify(await(group, deadline).authorizedOperations());
      } catch (ExecutionException e) {
        groupStatus = classifyFailure(e.getCause() == null ? e : e.getCause());
      } catch (TimeoutException e) {
        groupStatus = Preflight.Status.unreachable("no answer within " + timeout);
      }
      Map<String, Preflight.Status> topicStatuses = new LinkedHashMap<>();
      for (Map.Entry<String, KafkaFuture<TopicDescription>> topic : described.entrySet()) {
        Preflight.Status status;
        try {
          status = classifyTopic(await(topic.getValue(), deadline).authorizedOperations());
        } catch (ExecutionException | TimeoutException e) {
          // Missing, not visible, or no answer: the source's start reports each of these.
          status = Preflight.Status.OK;
        }
        topicStatuses.put(topic.getKey(), status);
      }
      return new Preflight.SourceAccess(groupStatus, topicStatuses);
    } catch (InterruptedException e) {
      // A stop interrupts preflight; its verdict is discarded.
      Thread.currentThread().interrupt();
      return new Preflight.SourceAccess(Preflight.Status.unreachable("interrupted"), Map.of());
    } finally {
      close(admin);
    }
  }

  /** Waits for an answer until the deadline; one that is already in is returned after it too. */
  private static <T> T await(KafkaFuture<T> future, long deadline)
      throws InterruptedException, ExecutionException, TimeoutException {
    return future.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
  }

  /**
   * The verdict for an existing group's authorized operations.
   *
   * @param operations what the broker lists, or {@code null} when it lists nothing (an old broker)
   * @return OK when the identity may read the group or the broker did not say
   */
  static Preflight.Status classify(@Nullable Set<AclOperation> operations) {
    if (operations == null
        || operations.contains(AclOperation.READ)
        || operations.contains(AclOperation.ALL)) {
      return Preflight.Status.OK;
    }
    return Preflight.Status.rejected("identity may describe the group but not read it");
  }

  /**
   * The verdict for an existing topic's authorized operations.
   *
   * @param operations what the broker lists, or {@code null} when it lists nothing (an old broker)
   * @return OK when the identity may read the topic or the broker did not say
   */
  static Preflight.Status classifyTopic(@Nullable Set<AclOperation> operations) {
    if (operations == null
        || operations.contains(AclOperation.READ)
        || operations.contains(AclOperation.ALL)) {
      return Preflight.Status.OK;
    }
    return Preflight.Status.rejected("identity may describe the topic but not read it");
  }

  /**
   * The verdict for a failed describe.
   *
   * @param failure the cause the admin client reported for the group
   * @return the verdict
   */
  static Preflight.Status classifyFailure(Throwable failure) {
    if (failure instanceof GroupIdNotFoundException
        || failure instanceof UnsupportedVersionException) {
      // A group never joined, or a broker that cannot say: the first join is the check.
      return Preflight.Status.OK;
    }
    if (failure instanceof AuthorizationException) {
      return Preflight.Status.rejected("identity may not use the group (" + failure + ")");
    }
    if (failure instanceof AuthenticationException) {
      return Preflight.Status.rejected("identity does not authenticate (" + failure + ")");
    }
    if (failure instanceof IllegalArgumentException) {
      // The group id is taken by another protocol (Connect, share group): no consumer can join.
      return Preflight.Status.rejected("group is not a consumer group (" + failure + ")");
    }
    return Preflight.Status.unreachable(failure.toString());
  }

  /**
   * The consumer settings without the consumer-only keys, with the admin client's own client id.
   * Everything else is kept, including keys neither client declares: config provider definitions
   * ({@code config.providers.<name>.class}) that resolve {@code ${file:...}} placeholders in the
   * credentials, and keys custom login or callback handlers read.
   */
  static Map<String, Object> adminSettings(Map<String, Object> consumer, String clientId) {
    Set<String> adminKeys = AdminClientConfig.configNames();
    Set<String> consumerKeys = ConsumerConfig.configNames();
    Map<String, Object> settings = new HashMap<>();
    consumer.forEach((key, value) -> {
      if (adminKeys.contains(key) || !consumerKeys.contains(key)) {
        settings.put(key, value);
      }
    });
    settings.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
    return settings;
  }

  /**
   * Closes the admin client with the interrupt flag cleared, then restores it: a stop interrupts a
   * check in progress, and an interrupted close would stop waiting for the client's I/O thread and
   * leave it running. No grace period: the verdict is in, or the check was abandoned on a timeout
   * or a stop, so a pending describe is failed at once. The close still joins the I/O thread,
   * without a bound: that relies on the thread exiting promptly once woken, as it does when only
   * calls are pending. A grace period would make a stop wait it out against a broker that never
   * answers.
   */
  private static void close(Admin admin) {
    boolean interrupted = Thread.interrupted();
    try {
      admin.close(Duration.ZERO);
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
