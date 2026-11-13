package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * The consumer group and topic check against a broker that accepts connections and never answers
 * (DESIGN §4.6): a stop interrupts the check while it waits.
 */
class KafkaGroupCheckTest {

  private static final String CLIENT_ID = "kafkafim-interrupted-preflight";

  private ServerSocket silent;
  private Thread acceptor;
  private final List<Socket> accepted = new CopyOnWriteArrayList<>();

  @BeforeEach
  void listen() throws IOException {
    silent = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
    acceptor =
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
  }

  @AfterEach
  void hangUp() throws Exception {
    silent.close();
    acceptor.join();
    for (Socket socket : accepted) {
      socket.close();
    }
  }

  @Test
  void interruptWhileWaitingOnTheBrokerAnswersTransientAtOnceAndLeavesNoThread() throws Exception {
    AtomicReference<Preflight.SourceAccess> access = new AtomicReference<>();
    AtomicBoolean stillInterrupted = new AtomicBoolean();
    Thread checker =
        Thread.ofPlatform()
            .start(() -> {
              access.set(
                  KafkaGroupCheck.check(
                      new DefaultKafkaConsumerFactory<>(
                          Map.<String, Object>of(
                              "bootstrap.servers", "127.0.0.1:" + silent.getLocalPort())),
                      "kafkafim.p",
                      List.of("t"),
                      CLIENT_ID,
                      Duration.ofSeconds(30)));
              stillInterrupted.set(Thread.currentThread().isInterrupted());
            });
    // The admin client has connected and the check waits for the describe (30s timeout).
    await()
        .atMost(Duration.ofSeconds(10))
        .until(() -> !accepted.isEmpty() && checker.getState() == Thread.State.TIMED_WAITING);
    assertThat(adminThreads()).isNotEmpty();

    long began = System.nanoTime();
    checker.interrupt();
    checker.join(Duration.ofSeconds(20));

    Duration took = Duration.ofNanos(System.nanoTime() - began);

    assertThat(checker.isAlive()).isFalse();
    assertThat(took).isLessThan(Duration.ofSeconds(5));
    assertThat(access.get().group()).isEqualTo(Preflight.Status.unreachable("interrupted"));
    assertThat(access.get().topics()).isEmpty();
    assertThat(stillInterrupted).isTrue();
    // The close joined the I/O thread although the flag was set.
    assertThat(adminThreads()).isEmpty();
  }

  private static List<String> adminThreads() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .map(Thread::getName)
        .filter(name -> name.equals("kafka-admin-client-thread | " + CLIENT_ID))
        .toList();
  }
}
