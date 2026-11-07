package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class OffsetTrackerTest {

  private static final TopicPartition P0 = new TopicPartition("t", 0);
  private static final TopicPartition P1 = new TopicPartition("t", 1);
  private static final Object C1 = new Object();
  private static final Object C2 = new Object();

  private final List<Boolean> budgetEvents = new ArrayList<>();
  private final ByteBudget budget = new ByteBudget(1_000_000, 0, budgetEvents::add);
  private final OffsetTracker tracker = new OffsetTracker(budget);

  private SourceRecord track(TopicPartition tp, long offset) {
    return track(tp, offset, new byte[10]);
  }

  private SourceRecord track(TopicPartition tp, long offset, byte[] value) {
    return tracker.track(
        C1, new ConsumerRecord<>(tp.topic(), tp.partition(), offset, null, value), 1_000);
  }

  private Long committable(TopicPartition tp) {
    OffsetAndMetadata offset = tracker.committable(C1, List.of(tp)).get(tp);
    return offset == null ? null : offset.offset();
  }

  @Test
  void commitsOnlyUpToTheLowestIncompleteOffset() {
    List<SourceRecord> records = new ArrayList<>();
    for (long offset = 0; offset < 10; offset++) {
      records.add(track(P0, offset));
    }
    assertThat(committable(P0)).as("nothing complete yet").isEqualTo(0L);

    for (int i : new int[] {0, 1, 2, 5, 6, 7, 9}) {
      records.get(i).complete();
    }
    assertThat(committable(P0)).isEqualTo(3L);

    records.get(4).complete();
    assertThat(committable(P0)).as("3 still blocks").isEqualTo(3L);

    records.get(3).complete();
    assertThat(committable(P0)).isEqualTo(8L);

    records.get(8).complete();
    assertThat(committable(P0)).as("all done: one past the highest").isEqualTo(10L);
    assertThat(tracker.pendingRecords()).isZero();
    assertThat(budget.outstanding()).isZero();
  }

  @Test
  void offsetGapsAreSkippedNaturally() {
    SourceRecord a = track(P0, 3);
    final SourceRecord b = track(P0, 17);
    SourceRecord c = track(P0, 40);

    c.complete();
    a.complete();
    assertThat(committable(P0)).isEqualTo(17L);
    b.complete();
    assertThat(committable(P0)).isEqualTo(41L);
  }

  @Test
  void partitionsAreIndependent() {
    final SourceRecord p0 = track(P0, 0);
    SourceRecord p1 = track(P1, 100);
    p1.complete();

    Map<TopicPartition, OffsetAndMetadata> offsets = tracker.committable(C1, List.of(P0, P1));
    assertThat(offsets.get(P0).offset()).isZero();
    assertThat(offsets.get(P1).offset()).isEqualTo(101L);
    assertThat(tracker.committable(C1, List.of(P0))).containsOnlyKeys(P0);
    assertThat(p0.isComplete()).isFalse();
  }

  @Test
  void committedOffsetIsNotOfferedAgain() {
    track(P0, 0).complete();
    Map<TopicPartition, OffsetAndMetadata> offsets = tracker.committable(C1, List.of(P0));
    tracker.committed(C1, offsets);
    assertThat(tracker.committable(C1, List.of(P0))).isEmpty();

    track(P0, 1).complete();
    assertThat(committable(P0)).isEqualTo(2L);
  }

  @Test
  void multiPartRecordCompletesOnceAllPartsDo() {
    SourceRecord record = track(P0, 0);
    record.expectParts(3);
    record.complete();
    record.complete();
    assertThat(record.isComplete()).isFalse();
    assertThat(committable(P0)).isZero();

    record.complete();
    assertThat(record.isComplete()).isTrue();
    assertThat(committable(P0)).isEqualTo(1L);

    record.complete();
    assertThat(tracker.pendingRecords()).as("extra completions are ignored").isZero();
  }

  @Test
  void zeroPartsCompletesImmediately() {
    SourceRecord record = track(P0, 0);
    record.expectParts(0);
    assertThat(record.isComplete()).isTrue();
    assertThat(committable(P0)).isEqualTo(1L);
  }

  @Test
  void partsCanBeDeclaredOnlyOnceAndBeforeCompleting() {
    SourceRecord declared = track(P0, 0);
    declared.expectParts(2);
    assertThatThrownBy(() -> declared.expectParts(2)).isInstanceOf(IllegalStateException.class);

    SourceRecord completed = track(P0, 1);
    completed.complete();
    assertThatThrownBy(() -> completed.expectParts(1)).isInstanceOf(IllegalStateException.class);

    assertThatThrownBy(() -> track(P0, 2).expectParts(-1))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void revokeKeepsCompletedOffsetsAndIgnoresLateCompletions() {
    SourceRecord r0 = track(P0, 0);
    final SourceRecord r1 = track(P0, 1);
    SourceRecord r2 = track(P0, 2);
    final SourceRecord other = track(P1, 0);
    r0.complete();
    r2.complete();
    assertThat(committable(P0)).isEqualTo(1L);
    assertThat(budget.outstanding()).isEqualTo(20);

    assertThat(tracker.revoke(C1, List.of(P0))).as("abandoned").isEqualTo(1);
    assertThat(tracker.committable(C1, List.of(P0))).isEmpty();
    assertThat(budget.outstanding()).as("only P1 left").isEqualTo(10);

    r1.complete();
    r1.expectParts(5); // abandoned: ignored, no exception
    assertThat(r1.isComplete()).isFalse();
    assertThat(budget.outstanding()).as("late completion releases nothing twice").isEqualTo(10);
    assertThat(tracker.pendingRecords()).isEqualTo(1);

    // A new assignment of P0 starts from scratch and is not affected by the old records.
    SourceRecord again = track(P0, 1);
    r1.complete();
    assertThat(committable(P0)).isEqualTo(1L);
    again.complete();
    assertThat(committable(P0)).isEqualTo(2L);
    other.complete();
    assertThat(tracker.pendingRecords()).isZero();
  }

  @Test
  void partitionMovedToAnotherConsumerIsNotDroppedByTheOldOwner() {
    tracker.assign(C1, List.of(P0));
    final SourceRecord old = track(P0, 100);

    // P0 moves to C2 in the same source before C1 learns it lost it; C2 re-reads from 100.
    tracker.assign(C2, List.of(P0));
    assertThat(old.isComplete()).isFalse();
    final SourceRecord fresh100 = tracker.track(C2, record(P0, 100), 1_000);
    final SourceRecord fresh101 = tracker.track(C2, record(P0, 101), 1_000);

    // The late lost/revoke callback on C1 must not touch C2's state.
    assertThat(tracker.tracked(C1, List.of(P0))).isEmpty();
    assertThat(tracker.revoke(C1, List.of(P0))).isZero();
    assertThat(tracker.committable(C1, List.of(P0))).as("C1 owns nothing").isEmpty();

    fresh101.complete();
    assertThat(tracker.committable(C2, List.of(P0)).get(P0).offset())
        .as("100 is still in flight on C2")
        .isEqualTo(100L);
    old.complete(); // abandoned when C2 took over: ignored
    assertThat(tracker.committable(C2, List.of(P0)).get(P0).offset()).isEqualTo(100L);
    fresh100.complete();
    assertThat(tracker.committable(C2, List.of(P0)).get(P0).offset()).isEqualTo(102L);
    assertThat(tracker.pendingRecords()).isZero();
    assertThat(budget.outstanding()).isZero();
  }

  @Test
  void revokeAllDropsStateOfEveryConsumer() {
    track(P0, 0);
    tracker.track(C2, record(P1, 0), 1_000);
    assertThat(tracker.revokeAll()).isEqualTo(2);
    assertThat(tracker.pendingRecords()).isZero();
    assertThat(budget.outstanding()).isZero();
  }

  private static ConsumerRecord<byte[], byte[]> record(TopicPartition tp, long offset) {
    return new ConsumerRecord<>(tp.topic(), tp.partition(), offset, null, new byte[10]);
  }

  @Test
  void oversizeIsFlaggedFromTheValueLength() {
    SourceRecord small = track(P0, 0, new byte[1_000]);
    SourceRecord big = track(P0, 1, new byte[1_001]);
    SourceRecord tombstone = track(P0, 2, null);
    assertThat(small.oversize()).isFalse();
    assertThat(big.oversize()).isTrue();
    assertThat(tombstone.oversize()).isFalse();
    assertThat(tombstone.sizeBytes()).isZero();
  }

  @Test
  void sizeCountsKeyAndValue() {
    SourceRecord record =
        tracker.track(C1, new ConsumerRecord<>("t", 0, 0, new byte[7], new byte[5]), 1_000);
    assertThat(record.sizeBytes()).isEqualTo(12);
    assertThat(budget.outstanding()).isEqualTo(12);
  }

  @Test
  void awaitIdleReturnsOnceEverythingCompleted() throws Exception {
    SourceRecord record = track(P0, 0);
    assertThat(tracker.awaitIdle(Duration.ofMillis(20))).isFalse();
    Thread completer = Thread.ofVirtual().start(record::complete);
    assertThat(tracker.awaitIdle(Duration.ofSeconds(5))).isTrue();
    completer.join();
  }

  @Test
  void concurrentCompletionAndRevokeReleaseEachRecordOnce() throws Exception {
    for (int round = 0; round < 50; round++) {
      List<SourceRecord> records = new ArrayList<>();
      for (int i = 0; i < 200; i++) {
        records.add(track(P0, round * 1_000L + i));
      }
      CountDownLatch start = new CountDownLatch(1);
      try (ExecutorService pool = Executors.newFixedThreadPool(4)) {
        for (SourceRecord record : records) {
          pool.submit(() -> {
            start.await();
            record.complete();
            return null;
          });
        }
        start.countDown();
        tracker.revoke(C1, List.of(P0));
        pool.shutdown();
        assertThat(pool.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      }
      assertThat(tracker.pendingRecords()).isZero();
      assertThat(budget.outstanding()).isZero();
    }
  }
}
