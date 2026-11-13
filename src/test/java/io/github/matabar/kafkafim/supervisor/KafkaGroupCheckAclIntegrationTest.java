package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.KafkaTopics;
import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.source.kafka.ConsumerFactories;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.ConsumerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * The consumer group preflight check (DESIGN §4.6, ADR-0038) against a broker that enforces ACLs
 * (SASL/PLAIN, {@code StandardAuthorizer}), with the source identity's consumer settings as
 * production builds them. Without the check a denied join leaves the pipeline {@code running}: the
 * listener container only publishes {@code ConsumerRetryAuthEvent} every auth retry interval and
 * never stops, so the {@code failed} state (ADR-0036) does not see it either.
 */
@Testcontainers
class KafkaGroupCheckAclIntegrationTest {

  private static final String ADMIN = "admin";
  private static final String ADMIN_PASSWORD = "admin-secret";
  private static final String APP = "app";
  private static final String APP_PASSWORD = "app-secret";
  private static final String TOPIC = "orders";
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  /** As in {@code DlqWriterAclIntegrationTest}: SASL on the client listener only. */
  @Container
  static final KafkaContainer KAFKA =
      KafkaTestContainers.jvmKafka()
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
          .withEnv(
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
              "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
              "org.apache.kafka.common.security.plain.PlainLoginModule required"
                  + " user_"
                  + ADMIN
                  + "=\""
                  + ADMIN_PASSWORD
                  + "\" user_"
                  + APP
                  + "=\""
                  + APP_PASSWORD
                  + "\";")
          .withEnv(
              "KAFKA_AUTHORIZER_CLASS_NAME",
              "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
          .withEnv("KAFKA_SUPER_USERS", "User:" + ADMIN + ";User:ANONYMOUS");

  private static Admin admin;
  private static ConsumerFactory<byte[], byte[]> factory;

  @BeforeAll
  static void setUp() throws Exception {
    admin = Admin.create(adminSettings());
    KafkaTopics.create(admin, List.of(new NewTopic(TOPIC, 1, (short) 1)));
    grant(ResourceType.TOPIC, TOPIC, AclOperation.READ);
    KafkaCluster cluster =
        new KafkaCluster(
            List.of(KAFKA.getBootstrapServers()),
            KafkaCluster.SecurityProtocol.SASL_PLAINTEXT,
            null,
            Map.of(
                APP,
                new KafkaCluster.KafkaIdentity(
                    new KafkaCluster.Sasl("PLAIN", APP, APP_PASSWORD), null)),
            APP,
            null);
    factory =
        new ConsumerFactories(
                new KafkafimProperties(
                    false, new KafkafimProperties.Kafka(Map.of("local", cluster)), null, Map.of()))
            .get("local", APP);
  }

  @AfterAll
  static void tearDown() {
    admin.close();
  }

  @Test
  void groupWithoutAnyAclIsRejected() {
    Preflight.Status status = check("kafkafim.no-acl-" + UUID.randomUUID());

    assertThat(status.kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(status.detail()).contains("may not use the group");
  }

  @Test
  void describeOnlyOnExistingGroupIsRejectedUntilReadIsGranted() throws Exception {
    String group = "kafkafim.describe-only-" + UUID.randomUUID();
    createGroup(group);
    grant(ResourceType.GROUP, group, AclOperation.DESCRIBE);
    // Until the ACL reaches the broker's authorizer, FindCoordinator is still denied.
    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(
            () -> !Objects.toString(check(group).detail(), "").contains("may not use the group"));

    assertThat(check(group))
        .isEqualTo(Preflight.Status.rejected("identity may describe the group but not read it"));

    grant(ResourceType.GROUP, group, AclOperation.READ);
    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> check(group).kind() == Preflight.Kind.OK);
  }

  @Test
  void readOnExistingGroupPasses() throws Exception {
    String group = "kafkafim.read-" + UUID.randomUUID();
    createGroup(group);
    grant(ResourceType.GROUP, group, AclOperation.READ);

    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> check(group).kind() == Preflight.Kind.OK);
  }

  /**
   * A group never joined has no description, so {@code Describe} cannot be told from {@code Read}:
   * the check passes and the first join stays the check for that one case.
   */
  @Test
  void describeOnNewGroupPasses() throws Exception {
    String group = "kafkafim.new-" + UUID.randomUUID();
    grant(ResourceType.GROUP, group, AclOperation.DESCRIBE);

    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> check(group).kind() == Preflight.Kind.OK);
  }

  /**
   * The topic half of the check (ADR-0039): {@code Describe} without {@code Read} on a source topic
   * passes the source's metadata fetch, and is rejected here. A topic the identity cannot see, and
   * one that does not exist, are left to the source's start.
   */
  @Test
  void describeOnlyOnTopicIsRejectedUntilReadIsGranted() throws Exception {
    String group = "kafkafim.topics-" + UUID.randomUUID();
    grant(ResourceType.GROUP, group, AclOperation.READ);
    String describeOnly = "describe-only-" + UUID.randomUUID();
    String hidden = "hidden-" + UUID.randomUUID();
    String absent = "absent-" + UUID.randomUUID();
    admin
        .createTopics(
            List.of(new NewTopic(describeOnly, 1, (short) 1), new NewTopic(hidden, 1, (short) 1)))
        .all()
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
    grant(ResourceType.TOPIC, describeOnly, AclOperation.DESCRIBE);
    grant(ResourceType.TOPIC, absent, AclOperation.READ);
    List<String> topics = List.of(TOPIC, describeOnly, hidden, absent);

    Preflight.Status rejected =
        Preflight.Status.rejected("identity may describe the topic but not read it");
    // Until the ACLs reach the broker's authorizer, the group and the topic are still denied.
    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> {
          Preflight.SourceAccess access = check(group, topics);
          return access.group().kind() == Preflight.Kind.OK
              && rejected.equals(access.topics().get(describeOnly));
        });

    assertThat(check(group, topics).topics())
        .containsOnly(
            Map.entry(TOPIC, Preflight.Status.OK),
            Map.entry(describeOnly, rejected),
            Map.entry(hidden, Preflight.Status.OK),
            Map.entry(absent, Preflight.Status.OK));

    grant(ResourceType.TOPIC, describeOnly, AclOperation.READ);
    await()
        .atMost(TIMEOUT)
        .pollInterval(Duration.ofMillis(500))
        .until(() -> check(group, topics).topics().get(describeOnly).kind() == Preflight.Kind.OK);
  }

  private static Preflight.SourceAccess check(String group, List<String> topics) {
    return KafkaGroupCheck.check(factory, group, topics, "kafkafim-test-preflight", TIMEOUT);
  }

  private static Preflight.Status check(String group) {
    return KafkaGroupCheck.check(factory, group, "kafkafim-test-preflight", TIMEOUT);
  }

  /** A group exists (empty, classic) once it has committed offsets. */
  private static void createGroup(String group) throws Exception {
    admin
        .alterConsumerGroupOffsets(
            group, Map.of(new TopicPartition(TOPIC, 0), new OffsetAndMetadata(0)))
        .all()
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static void grant(ResourceType type, String name, AclOperation operation)
      throws Exception {
    admin
        .createAcls(
            List.of(
                new AclBinding(
                    new ResourcePattern(type, name, PatternType.LITERAL),
                    new AccessControlEntry(
                        "User:" + APP, "*", operation, AclPermissionType.ALLOW))))
        .all()
        .get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
  }

  private static Map<String, Object> adminSettings() {
    return Map.of(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        KAFKA.getBootstrapServers(),
        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
        "SASL_PLAINTEXT",
        SaslConfigs.SASL_MECHANISM,
        "PLAIN",
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
            + ADMIN
            + "\" password=\""
            + ADMIN_PASSWORD
            + "\";");
  }
}
