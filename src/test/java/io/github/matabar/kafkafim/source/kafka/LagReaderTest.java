package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class LagReaderTest {

  private static final TopicPartition P0 = new TopicPartition("orders", 0);
  private static final TopicPartition P1 = new TopicPartition("orders", 1);
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  /** A consumer whose committed-offset request hangs like an unreachable broker until woken. */
  static final class HangingConsumer extends MockConsumer<byte[], byte[]> {
    final CountDownLatch woken = new CountDownLatch(1);
    final AtomicBoolean asked = new AtomicBoolean();

    HangingConsumer() {
      super("earliest");
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(
        Set<TopicPartition> partitions, Duration timeout) {
      asked.set(true);
      try {
        if (woken.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
          throw new WakeupException();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      throw new TimeoutException("committed offsets not read within " + timeout);
    }

    @Override
    public void wakeup() {
      woken.countDown();
    }
  }

  @Test
  void lagIsEndOffsetMinusCommittedAndPartitionsWithoutCommitAreLeftOut() {
    MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
    consumer.updateEndOffsets(Map.of(P0, 100L, P1, 7L));
    consumer.assign(Set.of(P0, P1));
    consumer.commitSync(Map.of(P0, new OffsetAndMetadata(60)));
    LagReader reader = new LagReader("orders", () -> consumer, () -> true);

    assertThat(reader.read(Set.of(P0, P1), TIMEOUT)).containsExactly(Map.entry(P0, 40L));
  }

  @Test
  void stoppedSourceReadsNothing() {
    AtomicBoolean created = new AtomicBoolean();
    LagReader reader =
        new LagReader(
            "orders",
            () -> {
              created.set(true);
              return new MockConsumer<>("earliest");
            },
            () -> false);

    assertThat(reader.read(Set.of(P0), TIMEOUT)).isEmpty();
    assertThat(created).isFalse();
  }

  @Test
  @Timeout(10)
  void stopWakesUpTheRequestInProgress() throws Exception {
    HangingConsumer consumer = new HangingConsumer();
    AtomicBoolean running = new AtomicBoolean(true);
    LagReader reader = new LagReader("orders", () -> consumer, running::get);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread poller =
        Thread.ofPlatform()
            .start(() -> {
              try {
                reader.read(Set.of(P0), Duration.ofSeconds(30));
              } catch (RuntimeException e) {
                failure.set(e);
              }
            });
    while (!consumer.asked.get()) {
      Thread.onSpinWait();
    }

    long start = System.nanoTime();
    running.set(false);
    reader.close();

    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(2));
    poller.join();
    assertThat(failure.get()).isInstanceOf(WakeupException.class);
    assertThat(consumer.closed()).isTrue();
  }

  /**
   * T20: a stop that begins while the lag consumer is being created finds nothing to wake up; the
   * read must notice the stop instead of holding the stop for two broker timeouts.
   */
  @Test
  @Timeout(10)
  void stopDuringConsumerCreationDoesNotWaitForTheBroker() throws Exception {
    HangingConsumer consumer = new HangingConsumer();
    AtomicBoolean running = new AtomicBoolean(true);
    AtomicReference<LagReader> reader = new AtomicReference<>();
    AtomicReference<Thread> stopper = new AtomicReference<>();
    AtomicReference<Duration> stopTook = new AtomicReference<>();
    reader.set(
        new LagReader(
            "orders",
            () -> {
              // The stop runs now: it marks the source stopped, finds no consumer to wake and
              // waits for the reader's lock.
              Thread stop =
                  Thread.ofPlatform()
                      .start(() -> {
                        long start = System.nanoTime();
                        running.set(false);
                        reader.get().close();
                        stopTook.set(Duration.ofNanos(System.nanoTime() - start));
                      });
              stopper.set(stop);
              while (stop.getState() != Thread.State.BLOCKED) {
                Thread.onSpinWait();
              }
              return consumer;
            },
            running::get));

    assertThat(reader.get().read(Set.of(P0), Duration.ofSeconds(5))).isEmpty();
    stopper.get().join();

    assertThat(consumer.asked).as("no broker request after the stop began").isFalse();
    assertThat(stopTook.get()).isLessThan(Duration.ofSeconds(2));
    assertThat(consumer.closed()).isTrue();
  }

  @Test
  void readAfterCloseCreatesAnotherConsumer() {
    AtomicReference<MockConsumer<byte[], byte[]>> last = new AtomicReference<>();
    LagReader reader =
        new LagReader(
            "orders",
            () -> {
              MockConsumer<byte[], byte[]> consumer = new MockConsumer<>("earliest");
              consumer.updateEndOffsets(Map.of(P0, 1L));
              last.set(consumer);
              return consumer;
            },
            () -> true);
    reader.read(Set.of(P0), TIMEOUT);
    MockConsumer<byte[], byte[]> first = last.get();
    reader.close();

    reader.read(Set.of(P0), TIMEOUT);

    assertThat(first.closed()).isTrue();
    assertThat(last.get()).isNotSameAs(first);
    assertThatThrownBy(() -> first.committed(Set.of(P0))).isInstanceOf(IllegalStateException.class);
  }

  /** A consumer whose committed-offset request takes a while, recording the end-offset bound. */
  static final class SlowConsumer extends MockConsumer<byte[], byte[]> {
    final Duration delay;
    final AtomicReference<Duration> endOffsetsTimeout = new AtomicReference<>();

    SlowConsumer(Duration delay) {
      super("earliest");
      this.delay = delay;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> committed(
        Set<TopicPartition> partitions, Duration timeout) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return Map.of();
    }

    @Override
    public synchronized Map<TopicPartition, Long> endOffsets(
        Collection<TopicPartition> partitions, Duration timeout) {
      endOffsetsTimeout.set(timeout);
      return Map.of();
    }
  }

  /** T21: the timeout bounds the whole read, so one pipeline's read cannot take two timeouts. */
  @Test
  void timeoutBoundsBothRequestsTogether() {
    SlowConsumer slow = new SlowConsumer(Duration.ofMillis(100));
    LagReader reader = new LagReader("orders", () -> slow, () -> true);
    reader.read(Set.of(P0), Duration.ofSeconds(1));
    assertThat(slow.endOffsetsTimeout.get()).isLessThanOrEqualTo(Duration.ofMillis(900));

    SlowConsumer slower = new SlowConsumer(Duration.ofMillis(300));
    LagReader late = new LagReader("orders", () -> slower, () -> true);
    assertThatThrownBy(() -> late.read(Set.of(P0), Duration.ofMillis(200)))
        .isInstanceOf(TimeoutException.class);
    assertThat(slower.endOffsetsTimeout.get()).as("no end-offset request").isNull();
  }
}
