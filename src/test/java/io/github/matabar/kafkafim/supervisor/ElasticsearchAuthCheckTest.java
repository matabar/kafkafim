package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The identity check against a cluster that accepts connections and never answers (DESIGN §4.6).
 */
class ElasticsearchAuthCheckTest {

  private ServerSocket silent;
  private Thread acceptor;
  private final List<Socket> accepted = new CopyOnWriteArrayList<>();
  private ElasticsearchAuthCheck check;

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
    check =
        new ElasticsearchAuthCheck(
            TestConfigs.bind(
                    """
                    kafkafim:
                      elasticsearch:
                        clusters:
                          es: { uris: ["http://127.0.0.1:%d"] }
                    """
                        .formatted(silent.getLocalPort()))
                .elasticsearch()
                .clusters());
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
  void anInterruptDuringTheRequestAnswersUnreachableAtOnce() throws Exception {
    AtomicReference<Preflight.Status> status = new AtomicReference<>();
    AtomicBoolean stillInterrupted = new AtomicBoolean();
    Thread checker =
        Thread.ofVirtual()
            .start(() -> {
              status.set(check.check("es", null));
              stillInterrupted.set(Thread.currentThread().isInterrupted());
            });
    await().atMost(Duration.ofSeconds(5)).until(() -> !accepted.isEmpty());
    Thread.sleep(200); // the request is sent and waits for an answer (10s response timeout)

    long began = System.nanoTime();
    checker.interrupt();
    checker.join(Duration.ofSeconds(10));

    // A graceful close of a client with the exchange still open would wait 5s for it.
    assertThat(Duration.ofNanos(System.nanoTime() - began)).isLessThan(Duration.ofSeconds(2));
    assertThat(status.get().kind()).isEqualTo(Preflight.Kind.TRANSIENT);
    assertThat(status.get().detail()).isEqualTo("check interrupted");
    assertThat(stillInterrupted).isTrue();
  }

  @Test
  void anAlreadyInterruptedThreadAnswersUnreachableAtOnce() throws Exception {
    AtomicReference<Preflight.Status> status = new AtomicReference<>();
    Thread checker =
        Thread.ofVirtual()
            .unstarted(() -> {
              Thread.currentThread().interrupt();
              status.set(check.check("es", null));
            });
    long began = System.nanoTime();
    checker.start();
    checker.join(Duration.ofSeconds(10));

    assertThat(Duration.ofNanos(System.nanoTime() - began)).isLessThan(Duration.ofSeconds(2));
    assertThat(status.get().kind()).isEqualTo(Preflight.Kind.TRANSIENT);
  }

  @Test
  void statusesAreClassified() {
    assertThat(ElasticsearchAuthCheck.classify(401).kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(ElasticsearchAuthCheck.classify(403).kind()).isEqualTo(Preflight.Kind.CONFIGURATION);
    assertThat(ElasticsearchAuthCheck.classify(503).kind()).isEqualTo(Preflight.Kind.TRANSIENT);
    assertThat(ElasticsearchAuthCheck.classify(405).kind()).isEqualTo(Preflight.Kind.OK);
  }
}
