package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Change detection on mounted directories (DESIGN §5.6). */
class ConfigWatcherTest {

  @TempDir Path dir;

  private final AtomicInteger changes = new AtomicInteger();
  private ConfigWatcher watcher;

  @AfterEach
  void close() {
    if (watcher != null) {
      watcher.close();
    }
  }

  private ConfigWatcher watch(Path root, Duration debounce) {
    watcher =
        new ConfigWatcher(
            List.of(root), Duration.ofMillis(200), debounce, changes::incrementAndGet);
    watcher.start();
    return watcher;
  }

  /** Writes {@code files} the way kubelet updates a ConfigMap or Secret volume. */
  private static void kubeletUpdate(Path volume, String generation, String file, String content)
      throws IOException {
    Path data = Files.createDirectories(volume.resolve("..2026_11_08_" + generation));
    Files.writeString(data.resolve(file), content);
    Path tmp = volume.resolve("..data_tmp");
    Files.deleteIfExists(tmp);
    Files.createSymbolicLink(tmp, data.getFileName());
    Files.move(tmp, volume.resolve("..data"), StandardCopyOption.ATOMIC_MOVE);
    Path link = volume.resolve(file);
    if (!Files.isSymbolicLink(link)) {
      Files.createSymbolicLink(link, Path.of("..data").resolve(file));
    }
  }

  @Test
  void kubeletSymlinkSwapIsOneChange() throws Exception {
    Path volume = Files.createDirectories(dir.resolve("config"));
    kubeletUpdate(volume, "a", "application-kafkafim.yaml", "kafkafim: {}\n");
    watch(volume, Duration.ofMillis(100));

    kubeletUpdate(volume, "b", "application-kafkafim.yaml", "kafkafim: { pipelines: {} }\n");
    await().atMost(Duration.ofSeconds(10)).until(() -> changes.get() == 1);
    Thread.sleep(800);
    assertThat(changes).hasValue(1);
  }

  @Test
  void anUnchangedContentIsNoChange() throws Exception {
    Path volume = Files.createDirectories(dir.resolve("config"));
    kubeletUpdate(volume, "a", "f.yaml", "same\n");
    watch(volume, Duration.ofMillis(50));
    // A new kubelet generation with the same content: the ..-prefixed names are not hashed.
    kubeletUpdate(volume, "b", "f.yaml", "same\n");
    Thread.sleep(1000);
    assertThat(changes).hasValue(0);
  }

  @Test
  void writesWithinTheDebounceAreReadOnce() throws Exception {
    Path secrets = Files.createDirectories(dir.resolve("secrets"));
    Files.writeString(secrets.resolve("a"), "1");
    watch(secrets, Duration.ofMillis(700));
    Files.writeString(secrets.resolve("a"), "2");
    Thread.sleep(150);
    Files.createDirectories(secrets.resolve("nested"));
    Files.writeString(secrets.resolve("nested").resolve("b"), "3");
    await().atMost(Duration.ofSeconds(10)).until(() -> changes.get() == 1);
    Thread.sleep(1000);
    assertThat(changes).hasValue(1);
  }

  @Test
  void rootThatAppearsLaterIsChange() throws Exception {
    Path missing = dir.resolve("later");
    watch(missing, Duration.ofMillis(50));
    Thread.sleep(400);
    assertThat(changes).hasValue(0);
    Files.createDirectories(missing);
    Files.writeString(missing.resolve("x"), "1");
    await().atMost(Duration.ofSeconds(10)).until(() -> changes.get() >= 1);
  }

  @Test
  void failingCallbackKeepsTheWatcherAlive() throws Exception {
    Path file = Files.writeString(dir.resolve("app.yaml"), "1");
    watcher =
        new ConfigWatcher(
            List.of(file),
            Duration.ofMillis(100),
            Duration.ofMillis(50),
            () -> {
              changes.incrementAndGet();
              throw new IllegalStateException("boom");
            });
    watcher.start();
    Files.writeString(file, "2");
    await().atMost(Duration.ofSeconds(10)).until(() -> changes.get() == 1);
    Files.writeString(file, "3");
    await().atMost(Duration.ofSeconds(10)).until(() -> changes.get() == 2);
  }

  @Test
  void pathsComeFromFileAndConfigtreeLocations() {
    assertThat(
            ConfigWatcher.paths(
                List.of(
                    "optional:/etc/kafkafim/config/",
                    "configtree:/etc/kafkafim/secrets/,classpath:/extra/",
                    "file:./conf/app.yaml;optional:file:/opt/*/conf/",
                    "optional:file:.env[.properties]",
                    "/etc/kafkafim/config/")))
        .containsExactly(
            Path.of("/etc/kafkafim/config/"),
            Path.of("/etc/kafkafim/secrets/"),
            Path.of("./conf/app.yaml"),
            Path.of("/opt"),
            Path.of(".env"));
    assertThat(ConfigWatcher.paths(List.of("classpath:/", "optional:classpath:/config/")))
        .isEmpty();
  }
}
