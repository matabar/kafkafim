package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.github.matabar.kafkafim.KafkaTestContainers;
import io.github.matabar.kafkafim.config.KafkaCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * {@link KafkaPipelineSource} against a real broker: batching, out-of-order completion, the byte
 * budget, restart re-delivery, graceful drain, and two sources sharing a group (DESIGN §4.1–§4.3,
 * §7).
 */
@Testcontainers
class KafkaPipelineSourceIntegrationTest {

  @Container static final KafkaContainer KAFKA = KafkaTestContainers.kafka();

  private static final Duration COMMIT_INTERVAL = Duration.ofMillis(200);
  private static final Duration TIMEOUT = Duration.ofSeconds(60);

  private static Admin admin;
  private static KafkaProducer<byte[], byte[]> producer;
  private static ConsumerFactories factories;

  private final List<KafkaPipelineSource> started = new ArrayList<>();

  @BeforeAll
  static void connect() {
    admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()));
    producer =
        new KafkaProducer<>(
            Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                ByteArraySerializer.class));
    KafkaCluster local =
        new KafkaCluster(List.of(KAFKA.getBootstrapServers()), null, null, null, null, null);
    factories =
        new ConsumerFactories(
            new KafkafimProperties(
                false, new KafkafimProperties.Kafka(Map.of("local", local)), null, null));
  }

  @AfterAll
  static void disconnect() {
    producer.close();
    admin.close();
  }

  @AfterEach
  void stopSources() {
    started.forEach(KafkaPipelineSource::stop);
  }

  @Test
  void deliversBatchesFlagsOversizeAndCommitsCompletedRecords() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 30, 100);
    producer.send(new ProducerRecord<>(topic, 0, null, new byte[2_000])).get();
    Recorder recorder = new Recorder(SourceRecord::complete);

    KafkaPipelineSource source = start(source(topic, 10, DataSize.ofMegabytes(1)), recorder, 1);

    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 31);
    assertThat(recorder.batches)
        .allSatisfy(batch -> assertThat(batch).hasSizeLessThanOrEqualTo(10));
    assertThat(recorder.batches).anySatisfy(batch -> assertThat(batch).hasSizeGreaterThan(1));
    assertThat(recorder.records).extracting(SourceRecord::offset).isSorted();
    assertThat(recorder.records.get(29).oversize()).isFalse();
    assertThat(recorder.records.get(30).oversize()).isTrue();
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 31);
    assertThat(source.inFlightBytes()).isZero();
    assertThat(source.concurrency()).isEqualTo(1);
  }

  @Test
  void outOfOrderCompletionCommitsOnlyTheContiguousPrefix() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 10, 10);
    Recorder recorder = new Recorder(record -> {});
    start(source(topic, 500, DataSize.ofMegabytes(1)), recorder, 1);
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 10);
    List<SourceRecord> records = recorder.records;

    for (int i : new int[] {0, 1, 2, 5, 6, 7, 9}) {
      records.get(i).complete();
    }
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 3);
    Thread.sleep(COMMIT_INTERVAL.multipliedBy(4).toMillis());
    assertThat(committed(groupOf(topic), topic, 0)).as("3 and 4 are incomplete").isEqualTo(3);

    records.get(4).complete();
    Thread.sleep(COMMIT_INTERVAL.multipliedBy(4).toMillis());
    assertThat(committed(groupOf(topic), topic, 0)).as("3 is still incomplete").isEqualTo(3);

    records.get(3).complete();
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 8);
    records.get(8).complete();
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 10);
  }

  @Test
  void pausesFetchingAtTheByteBudgetAndResumesBelowIt() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 200, 100);
    Recorder recorder = new Recorder(record -> {});
    KafkaPipelineSource source = start(source(topic, 5, DataSize.ofBytes(1_000)), recorder, 1);

    await()
        .atMost(TIMEOUT)
        .until(() ->
            source.isConsumerPaused()
                && source.pauseReasons().contains(KafkaPipelineSource.BUDGET_REASON));
    int delivered = recorder.records.size();
    assertThat(delivered).as("budget plus at most one poll").isBetween(10, 14);
    Thread.sleep(1_500);
    assertThat(recorder.records).as("no fetching while paused").hasSize(delivered);

    // Completing everything delivered so far frees the budget; from then on, complete on arrival.
    recorder.onRecord = SourceRecord::complete;
    recorder.records.forEach(SourceRecord::complete);
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 200);
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 200);
    assertThat(source.pauseReasons()).isEmpty();
  }

  @Test
  void explicitPauseStopsFetchingUntilResumed() throws Exception {
    String topic = createTopic(1);
    Recorder recorder = new Recorder(SourceRecord::complete);
    KafkaPipelineSource source = start(source(topic, 500, DataSize.ofMegabytes(1)), recorder, 1);
    await().atMost(TIMEOUT).until(() -> !source.assignedPartitions().isEmpty());

    source.pause("breaker:search-eu");
    await().atMost(TIMEOUT).until(source::isConsumerPaused);
    produce(topic, 0, 5, 10);
    Thread.sleep(1_500);
    assertThat(recorder.records).isEmpty();
    assertThatThrownBy(() -> source.resume(KafkaPipelineSource.BUDGET_REASON))
        .isInstanceOf(IllegalArgumentException.class);

    source.resume("breaker:search-eu");
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 5);
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 5);
  }

  @Test
  void restartRedeliversUncommittedRecords() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 10, 10);
    Pipeline.Source config = source(topic, 500, DataSize.ofMegabytes(1));
    Recorder first = new Recorder(record -> {});
    final KafkaPipelineSource source = start(config, first, 1);
    await().atMost(TIMEOUT).until(() -> first.records.size() == 10);
    for (int i : new int[] {0, 1, 2, 3, 5}) {
      first.records.get(i).complete();
    }
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 4);

    source.stop();
    started.remove(source);
    assertThat(committed(groupOf(topic), topic, 0)).isEqualTo(4);
    first.records.get(6).complete(); // after stop: ignored

    Recorder second = new Recorder(SourceRecord::complete);
    start(config, second, 1);
    await().atMost(TIMEOUT).until(() -> second.records.size() == 6);
    assertThat(second.records)
        .extracting(SourceRecord::offset)
        .containsExactly(4L, 5L, 6L, 7L, 8L, 9L);
    await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, 0) == 10);
  }

  @Test
  void stopDrainsInFlightRecordsAndCommitsThem() throws Exception {
    String topic = createTopic(1);
    produce(topic, 0, 10, 10);
    Recorder recorder = new Recorder(record -> {});
    KafkaPipelineSource source =
        new KafkaPipelineSource(
            "drain",
            // A long commit-interval: the only commit after the first poll is the one on revoke.
            source(topic, 500, DataSize.ofMegabytes(1), Duration.ofHours(1)),
            factories.get("local", null),
            recorder,
            SourceOptions.defaults().withDesiredConcurrency(1).withDrainTimeout(TIMEOUT));
    source.start();
    started.add(source);
    await().atMost(TIMEOUT).until(() -> recorder.records.size() == 10);

    Thread stopper = Thread.ofVirtual().start(source::stop);
    await()
        .atMost(TIMEOUT)
        .until(() -> source.pauseReasons().contains(KafkaPipelineSource.STOPPING_REASON));
    Thread.sleep(300);
    assertThat(stopper.isAlive()).as("waits for in-flight records").isTrue();
    assertThat(committed(groupOf(topic), topic, 0)).as("nothing committed yet").isLessThan(1);
    recorder.records.forEach(SourceRecord::complete);
    stopper.join(TIMEOUT.toMillis());
    assertThat(stopper.isAlive()).isFalse();
    assertThat(committed(groupOf(topic), topic, 0)).isEqualTo(10);
  }

  @Test
  void missingTopicKeepsThePipelineStopped() throws Exception {
    String topic = "missing-" + UUID.randomUUID();
    KafkaPipelineSource source =
        new KafkaPipelineSource(
            "missing",
            source(topic, 500, DataSize.ofMegabytes(1)),
            factories.get("local", null),
            new Recorder(record -> {}),
            SourceOptions.defaults());
    assertThatThrownBy(source::start)
        .isInstanceOf(MissingTopicsException.class)
        .hasMessageContaining(topic);
    assertThat(source.isRunning()).isFalse();
    assertThat(admin.listTopics().names().get()).as("never created").doesNotContain(topic);
  }

  @Test
  void twoSourcesInOneGroupSplitPartitionsAndRevokeSafely() throws Exception {
    String topic = createTopic(4);
    Pipeline.Source config = source(topic, 500, DataSize.ofMegabytes(1));
    Recorder first = new Recorder(record -> {});
    KafkaPipelineSource a = start(config, first, 2);
    assertThat(a.concurrency()).isEqualTo(2);
    // a's consumers can form the group one at a time: the first may take all four partitions and
    // hand two to its sibling in a later rebalance. Wait for that split before producing, and
    // forget its revocations, so that b's arrival is the only rebalance asserted on below. If b
    // joined first, a's late consumer would take a partition from its sibling in b's rebalance,
    // and a would revoke a partition that b never receives.
    await()
        .atMost(TIMEOUT)
        .until(() -> {
          Map<String, Set<TopicPartition>> owners = stableAssignment(groupOf(topic));
          return owners.size() == 2 && owners.values().stream().allMatch(tps -> tps.size() == 2);
        });
    first.revoked.clear();
    for (int p = 0; p < 4; p++) {
      produce(topic, p, 10, 10);
    }
    await().atMost(TIMEOUT).until(() -> first.records.size() == 40);
    first.records.stream().filter(r -> r.offset() < 5).forEach(SourceRecord::complete);

    // Six consumers for four partitions: the sticky assignor must move two partitions to b.
    Recorder second = new Recorder(SourceRecord::complete);
    KafkaPipelineSource b = start(config, second, 8);
    assertThat(b.concurrency()).as("capped at the partition count").isEqualTo(4);

    await()
        .atMost(TIMEOUT)
        .until(() ->
            stableAssignment(groupOf(topic)).size() == 6
                && !a.assignedPartitions().isEmpty()
                && !b.assignedPartitions().isEmpty()
                && a.assignedPartitions().size() + b.assignedPartitions().size() == 4);
    Set<TopicPartition> union = new HashSet<>(a.assignedPartitions());
    union.addAll(b.assignedPartitions());
    assertThat(union).as("every partition exactly once").hasSize(4);
    Set<TopicPartition> moved = Set.copyOf(b.assignedPartitions());
    assertThat(first.revoked).containsExactlyInAnyOrderElementsOf(moved);

    // The new owner re-reads from the commit made on revoke: offsets 5..9 of each moved partition.
    await().atMost(TIMEOUT).until(() -> second.records.size() == 5 * moved.size());
    assertThat(second.records).allSatisfy(r -> assertThat(r.offset()).isBetween(5L, 9L));

    // Late completions on the old owner are ignored: they release nothing twice and commit nothing.
    first.records.stream()
        .filter(r -> moved.contains(r.topicPartition()))
        .forEach(SourceRecord::complete);
    for (TopicPartition tp : moved) {
      await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, tp.partition()) == 10);
    }
    first.records.forEach(SourceRecord::complete);
    for (int p = 0; p < 4; p++) {
      int partition = p;
      await().atMost(TIMEOUT).until(() -> committed(groupOf(topic), topic, partition) == 10);
    }
    assertThat(a.inFlightRecords()).isZero();
    assertThat(a.inFlightBytes()).isZero();
  }

  private KafkaPipelineSource start(Pipeline.Source config, Recorder recorder, int concurrency) {
    KafkaPipelineSource source =
        new KafkaPipelineSource(
            "it-" + config.topics().get(0),
            config,
            factories.get("local", null),
            recorder,
            SourceOptions.defaults()
                .withDesiredConcurrency(concurrency)
                .withDrainTimeout(Duration.ofMillis(300)));
    source.start();
    started.add(source);
    return source;
  }

  private static Pipeline.Source source(String topic, int maxPollRecords, DataSize maxInFlight) {
    return source(topic, maxPollRecords, maxInFlight, COMMIT_INTERVAL);
  }

  private static Pipeline.Source source(
      String topic, int maxPollRecords, DataSize maxInFlight, Duration commitInterval) {
    return new Pipeline.Source(
        "local",
        List.of(topic),
        groupOf(topic),
        null,
        Pipeline.OffsetReset.EARLIEST,
        maxPollRecords,
        maxInFlight,
        commitInterval,
        DataSize.ofBytes(1_000));
  }

  private static String groupOf(String topic) {
    return "kafkafim." + topic;
  }

  private static String createTopic(int partitions) throws Exception {
    String topic = "t-" + UUID.randomUUID().toString().substring(0, 8);
    admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
    return topic;
  }

  private static void produce(String topic, int partition, int count, int valueBytes)
      throws Exception {
    for (int i = 0; i < count; i++) {
      producer.send(new ProducerRecord<>(topic, partition, null, new byte[valueBytes]));
    }
    producer.flush();
  }

  private static long committed(String group, String topic, int partition) throws Exception {
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get();
    OffsetAndMetadata offset = offsets.get(new TopicPartition(topic, partition));
    return offset == null ? -1 : offset.offset();
  }

  /** The group's assignment by member id while it is stable, or empty while it is rebalancing. */
  private static Map<String, Set<TopicPartition>> stableAssignment(String group) throws Exception {
    ConsumerGroupDescription description =
        admin.describeConsumerGroups(List.of(group)).all().get().get(group);
    if (description.groupState() != GroupState.STABLE) {
      return Map.of();
    }
    Map<String, Set<TopicPartition>> owners = new HashMap<>();
    for (MemberDescription member : description.members()) {
      owners.put(member.consumerId(), member.assignment().topicPartitions());
    }
    return owners;
  }

  /** Records what the source delivers; {@code onRecord} runs for each record on arrival. */
  private static final class Recorder implements SourceRecordHandler {

    final List<List<SourceRecord>> batches = new CopyOnWriteArrayList<>();
    final List<SourceRecord> records = new CopyOnWriteArrayList<>();
    final List<TopicPartition> revoked = new CopyOnWriteArrayList<>();
    volatile Consumer<SourceRecord> onRecord;

    Recorder(Consumer<SourceRecord> onRecord) {
      this.onRecord = onRecord;
    }

    @Override
    public void onBatch(List<SourceRecord> batch) {
      batches.add(batch);
      records.addAll(batch);
      batch.forEach(onRecord);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      revoked.addAll(partitions);
    }
  }
}
