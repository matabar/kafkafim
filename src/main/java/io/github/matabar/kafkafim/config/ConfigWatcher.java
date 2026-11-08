package io.github.matabar.kafkafim.config;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches the configuration and secret directories and calls back once their content changed
 * (DESIGN §5.6). Kubelet updates a mounted {@code ConfigMap} or {@code Secret} by writing a new
 * timestamped directory and swapping the {@code ..data} symlink, up to about a minute after the API
 * object changed; file events for that swap are not reliable on every platform, so:
 *
 * <ul>
 *   <li>a {@link WatchService} on every directory (the roots and their subdirectories) wakes the
 *       watcher early;
 *   <li>every {@code pollInterval} (10s by default) and on every event, the watcher hashes the
 *       content of every regular file under the roots (following symlinks, skipping kubelet's
 *       {@code ..}-prefixed entries, whose names change with every update) and compares it with the
 *       last hash;
 *   <li>after a change it waits until the hash has been stable for {@code debounce}, so a kubelet
 *       update of several files is read once, then calls back on the watcher thread.
 * </ul>
 *
 * <p>The callback runs on the watcher's own thread, one call at a time. A root that does not exist
 * is hashed as absent, so a mount that appears later is a change too. The last hash is updated
 * before the callback, whatever it does: an invalid tree is reported once, not every poll.
 */
public final class ConfigWatcher implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigWatcher.class);

  private final List<Path> roots;
  private final Duration pollInterval;
  private final Duration debounce;
  private final Runnable onChange;
  private final Set<Path> registered = new HashSet<>();
  private final @Nullable WatchService watchService;
  private final Thread thread;
  private volatile boolean closed;
  private String lastHash;

  /**
   * Creates a watcher; nothing is watched until {@link #start()}.
   *
   * @param roots files or directories to watch
   * @param pollInterval how often to hash when no event arrives
   * @param debounce how long the content must stay unchanged before {@code onChange} runs
   * @param onChange called on the watcher thread after a change
   */
  public ConfigWatcher(
      List<Path> roots, Duration pollInterval, Duration debounce, Runnable onChange) {
    this.roots = List.copyOf(roots);
    this.pollInterval = pollInterval;
    this.debounce = debounce;
    this.onChange = onChange;
    WatchService service = null;
    try {
      service = FileSystems.getDefault().newWatchService();
    } catch (IOException | UnsupportedOperationException e) {
      LOG.warn("No file watch service ({}); relying on the {} poll", e, pollInterval);
    }
    this.watchService = service;
    this.thread = Thread.ofPlatform().name("kafkafim-config-watcher").daemon().unstarted(this::run);
    this.lastHash = hash();
  }

  /** The watched roots. */
  public List<Path> roots() {
    return roots;
  }

  /** Starts the watcher thread. */
  public void start() {
    LOG.info(
        "Watching {} for configuration changes (poll {}, debounce {})",
        roots,
        pollInterval,
        debounce);
    thread.start();
  }

  /** Stops watching. Does not wait for a callback in progress, which finishes on its own thread. */
  @Override
  public void close() {
    closed = true;
    if (watchService != null) {
      try {
        watchService.close();
      } catch (IOException e) {
        LOG.debug("Closing the watch service failed", e);
      }
    }
    if (Thread.currentThread() != thread) {
      thread.interrupt();
    }
  }

  private void run() {
    while (!closed) {
      try {
        awaitEventOrPoll();
        String current = hash();
        if (current.equals(lastHash)) {
          continue;
        }
        current = awaitStable(current);
        if (closed) {
          return;
        }
        lastHash = current;
        LOG.info("Configuration files under {} changed; reloading", roots);
        onChange.run();
      } catch (InterruptedException | ClosedWatchServiceException e) {
        if (closed) {
          return;
        }
      } catch (RuntimeException e) {
        LOG.error("Configuration watcher failed; continuing", e);
      }
    }
  }

  /** Waits for a file event (draining every pending one) or the poll interval. */
  private void awaitEventOrPoll() throws InterruptedException {
    if (watchService == null) {
      Thread.sleep(pollInterval.toMillis());
      return;
    }
    WatchKey key = watchService.poll(pollInterval.toMillis(), TimeUnit.MILLISECONDS);
    while (key != null) {
      key.pollEvents();
      if (!key.reset()) {
        registered.remove((Path) key.watchable());
      }
      key = watchService.poll();
    }
  }

  /** Re-hashes every {@code debounce} until two hashes in a row agree. */
  private String awaitStable(String changed) throws InterruptedException {
    String previous = changed;
    while (!closed) {
      Thread.sleep(debounce.toMillis());
      String current = hash();
      if (current.equals(previous)) {
        return current;
      }
      previous = current;
    }
    return previous;
  }

  /** SHA-256 over every root's sorted relative paths and file contents; registers directories. */
  String hash() {
    MessageDigest digest = sha256();
    for (Path root : roots) {
      digest.update(root.toString().getBytes(StandardCharsets.UTF_8));
      digest.update((byte) 0);
      TreeMap<String, Path> files = new TreeMap<>();
      if (Files.isRegularFile(root)) {
        files.put("", root);
        register(root.toAbsolutePath().getParent());
      } else if (Files.isDirectory(root)) {
        collect(root, files);
      } else {
        digest.update("<absent>".getBytes(StandardCharsets.UTF_8));
      }
      for (var file : files.entrySet()) {
        digest.update(file.getKey().getBytes(StandardCharsets.UTF_8));
        digest.update((byte) 0);
        try {
          digest.update(Files.readAllBytes(file.getValue()));
        } catch (IOException e) {
          // Vanished between listing and reading (a kubelet swap): hash the failure, so the
          // debounce re-reads until the tree is stable.
          digest.update(
              ("<unreadable:" + e.getClass().getSimpleName() + ">")
                  .getBytes(StandardCharsets.UTF_8));
        }
        digest.update((byte) 0);
      }
    }
    return HexFormat.of().formatHex(digest.digest());
  }

  private void collect(Path root, TreeMap<String, Path> files) {
    try {
      Files.walkFileTree(
          root,
          EnumSet.of(FileVisitOption.FOLLOW_LINKS),
          Integer.MAX_VALUE,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              if (!dir.equals(root) && isHidden(dir)) {
                return FileVisitResult.SKIP_SUBTREE;
              }
              register(dir);
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              if (attrs.isRegularFile() && !isHidden(file)) {
                files.put(root.relativize(file).toString(), file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static boolean isHidden(Path path) {
    Path name = path.getFileName();
    return name != null && name.toString().startsWith("..");
  }

  private void register(@Nullable Path dir) {
    if (watchService == null || dir == null || registered.contains(dir)) {
      return;
    }
    try {
      dir.register(
          watchService,
          StandardWatchEventKinds.ENTRY_CREATE,
          StandardWatchEventKinds.ENTRY_DELETE,
          StandardWatchEventKinds.ENTRY_MODIFY);
      registered.add(dir);
    } catch (IOException | ClosedWatchServiceException e) {
      LOG.debug("Cannot watch {}: {}", dir, e.toString());
    }
  }

  private static MessageDigest sha256() {
    try {
      return MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The filesystem paths named by Spring config locations ({@code spring.config.location}, {@code
   * spring.config.additional-location}, {@code spring.config.import}): {@code file:} and {@code
   * configtree:} locations and plain paths, with {@code optional:} and an {@code [.ext]} hint
   * stripped; for a wildcard location, the directory above the first {@code *}. {@code classpath:}
   * and other schemes are not files and are skipped.
   *
   * @param locations location values, each possibly a comma- or semicolon-separated list
   * @return distinct paths, in order
   */
  public static List<Path> paths(List<String> locations) {
    Set<Path> paths = new LinkedHashSet<>();
    for (String value : locations) {
      for (String raw : value.split("[,;]")) {
        Path path = path(raw.strip());
        if (path != null) {
          paths.add(path);
        }
      }
    }
    return new ArrayList<>(paths);
  }

  private static @Nullable Path path(String location) {
    String rest = location;
    if (rest.toLowerCase(Locale.ROOT).startsWith("optional:")) {
      rest = rest.substring("optional:".length());
    }
    for (String scheme : List.of("file:", "configtree:")) {
      if (rest.toLowerCase(Locale.ROOT).startsWith(scheme)) {
        rest = rest.substring(scheme.length());
        break;
      }
    }
    if (rest.isEmpty() || rest.matches("^[A-Za-z][A-Za-z0-9+.-]*:.*") && !isWindowsDrive(rest)) {
      return null;
    }
    int hint = rest.lastIndexOf('[');
    if (hint > 0 && rest.endsWith("]")) {
      rest = rest.substring(0, hint);
    }
    int wildcard = rest.indexOf('*');
    if (wildcard >= 0) {
      String prefix = rest.substring(0, wildcard);
      return prefix.endsWith("/") ? Path.of(prefix) : Path.of(prefix).getParent();
    }
    return Path.of(rest);
  }

  private static boolean isWindowsDrive(String path) {
    return path.length() > 2 && path.charAt(1) == ':' && Character.isLetter(path.charAt(0));
  }
}
