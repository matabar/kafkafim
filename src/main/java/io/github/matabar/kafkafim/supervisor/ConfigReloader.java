package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.ConfigDiff;
import io.github.matabar.kafkafim.config.ConfigLoader;
import io.github.matabar.kafkafim.config.ConfigWatcher;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;

/**
 * Hot reload (DESIGN §5.6): re-reads the configuration from disk ({@link FreshEnvironment}), binds
 * and validates it whole ({@link ConfigLoader}), diffs it by pipeline ({@link ConfigDiff}) and lets
 * the {@link PipelineSupervisor} apply it. The file watcher and {@code POST /actuator/reload} both
 * end here; reloads run one at a time, a second caller waits for the first.
 *
 * <p>An invalid or unreadable tree is rejected whole: the running configuration stays, {@code
 * kafkafim_config_reload_total{result="failure"}} increments and one {@code ERROR} record lists
 * every violation. A valid tree increments {@code result="success"}; if it changed anything, {@code
 * kafkafim_config_generation} (1 at startup) increments too. A valid tree, changed or not, also
 * starts once more every pipeline it leaves unchanged that is in {@code failed_preflight} or {@code
 * failed} ({@link PipelineSupervisor#retryFailedPipelines}); a rejected tree retries nothing.
 *
 * <p>As a {@link SmartLifecycle} one phase above the supervisor ({@link #PHASE}), it starts the
 * {@link ConfigWatcher} after the pipelines started and stops it before they stop.
 */
final class ConfigReloader implements SmartLifecycle {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigReloader.class);

  /** Starts after the supervisor ({@link PipelineSupervisor#PHASE}) and stops before it. */
  static final int PHASE = PipelineSupervisor.PHASE + 50;

  /** Counter name; Prometheus renders it as {@code kafkafim_config_reload_total}. */
  static final String RELOADS = "kafkafim.config.reload";

  /** Gauge name; Prometheus renders it as {@code kafkafim_config_generation}. */
  static final String GENERATION = "kafkafim.config.generation";

  /** The Spring config location properties the watcher derives its directories from. */
  static final List<String> LOCATION_PROPERTIES =
      List.of(
          "spring.config.location", "spring.config.additional-location", "spring.config.import");

  private final Supplier<Environment> environments;
  private final PipelineSupervisor supervisor;
  private final Function<List<Path>, @Nullable ConfigWatcher> watchers;
  private final List<Path> watched;
  private final Counter successes;
  private final Counter failures;
  private final AtomicLong generation = new AtomicLong(1);
  private final Object reloadLock = new Object();
  private volatile @Nullable ConfigWatcher watcher;
  private volatile boolean running;

  /**
   * Creates the reloader for a running application.
   *
   * @param environment the application's environment; its config data is re-read on every reload
   * @param supervisor applies valid trees
   * @param registry where the reload meters go
   */
  ConfigReloader(
      ConfigurableEnvironment environment, PipelineSupervisor supervisor, MeterRegistry registry) {
    this(
        () -> FreshEnvironment.load(environment),
        supervisor,
        registry,
        watchedPaths(environment),
        null);
  }

  /**
   * Creates a reloader.
   *
   * @param environments loads the current configuration from disk, throwing if it cannot
   * @param supervisor applies valid trees
   * @param registry where the reload meters go
   * @param watched the directories and files to watch
   * @param watchers creates the watcher for {@code watched}; {@code null} for the default
   */
  ConfigReloader(
      Supplier<Environment> environments,
      PipelineSupervisor supervisor,
      MeterRegistry registry,
      List<Path> watched,
      @Nullable Function<List<Path>, @Nullable ConfigWatcher> watchers) {
    this.environments = environments;
    this.supervisor = supervisor;
    this.watched = List.copyOf(watched);
    this.watchers = watchers != null ? watchers : this::defaultWatcher;
    this.successes = reloads(registry, "success");
    this.failures = reloads(registry, "failure");
    Gauge.builder(GENERATION, generation, AtomicLong::doubleValue)
        .description("Configuration generation in effect: 1 at startup, +1 per applied reload")
        .register(registry);
  }

  private static Counter reloads(MeterRegistry registry, String result) {
    return Counter.builder(RELOADS)
        .description("Configuration reloads by result (failure: rejected, running config kept)")
        .tag("result", result)
        .register(registry);
  }

  /** The files and directories named by the Spring config locations of {@code environment}. */
  static List<Path> watchedPaths(Environment environment) {
    Binder binder = Binder.get(environment);
    List<String> locations = new ArrayList<>();
    for (String property : LOCATION_PROPERTIES) {
      binder.bind(property, Bindable.listOf(String.class)).ifBound(locations::addAll);
    }
    return ConfigWatcher.paths(locations);
  }

  /** The configuration generation in effect. */
  long generation() {
    return generation.get();
  }

  /**
   * Reloads now, waiting for a reload in progress first.
   *
   * @param trigger what asked for it, for the log ({@code watcher}, {@code endpoint})
   * @return what happened
   * @throws IllegalStateException if the supervisor is not running
   */
  ReloadResult reload(String trigger) {
    synchronized (reloadLock) {
      if (!supervisor.isRunning()) {
        throw new IllegalStateException("The pipeline supervisor is not running");
      }
      KafkafimProperties running = supervisor.properties();
      ConfigLoader.Result loaded;
      try {
        loaded = ConfigLoader.load(environments.get());
      } catch (RuntimeException e) {
        loaded =
            new ConfigLoader.Result(null, List.of("configuration cannot be read: " + firstLine(e)));
      }
      if (!loaded.isValid()) {
        return rejected(trigger, loaded.problems());
      }
      KafkafimProperties next = loaded.properties();
      if (next.equals(running)) {
        ConfigDiff diff = ConfigDiff.between(running, next);
        List<String> retried = supervisor.retryFailedPipelines(diff.unchanged());
        successes.increment();
        LOG.info(
            "Configuration reload ({}): no change, generation {} stays{}",
            trigger,
            generation(),
            retriedSuffix(retried));
        return result(ReloadResult.UNCHANGED, diff, retried);
      }
      if (!next.reload().equals(running.reload())) {
        LOG.warn("kafkafim.reload.* changed; the watcher settings apply after a restart");
      }
      ConfigDiff diff = ConfigDiff.between(running, next);
      LOG.info(
          "Configuration reload ({}): applying generation {}: added {}, removed {}, restarted {},"
              + " unchanged {}",
          trigger,
          generation() + 1,
          diff.added(),
          diff.removed(),
          diff.restarted(),
          diff.unchanged());
      List<String> retried;
      try {
        retried = supervisor.apply(next, diff);
      } catch (RuntimeException e) {
        if (!supervisor.isRunning()) {
          throw e; // a shutdown began before apply took over: nothing was applied, no failure
        }
        failures.increment();
        LOG.error(
            "Configuration reload ({}) failed while applying generation {}; pipelines may be"
                + " partly reconfigured",
            trigger,
            generation() + 1,
            e);
        throw e;
      }
      generation.incrementAndGet();
      successes.increment();
      LOG.info(
          "Configuration reload ({}): generation {} applied{}",
          trigger,
          generation(),
          retriedSuffix(retried));
      return result(ReloadResult.APPLIED, diff, retried);
    }
  }

  private static String retriedSuffix(List<String> retried) {
    return retried.isEmpty() ? "" : "; retrying failed pipeline(s) " + retried;
  }

  /**
   * The exception type and the first line of its message: a YAML parser appends a snippet of the
   * offending file, which stays out of the response and the log.
   */
  static String firstLine(Throwable e) {
    if (e.getMessage() == null) {
      return e.getClass().getSimpleName();
    }
    String message = e.getMessage().strip();
    int newline = message.indexOf('\n');
    return e.getClass().getSimpleName()
        + ": "
        + (newline < 0 ? message : message.substring(0, newline).strip());
  }

  private ReloadResult rejected(String trigger, List<String> problems) {
    failures.increment();
    LOG.error(
        "Configuration reload ({}) rejected; generation {} keeps running. {} problem(s):\n  {}",
        trigger,
        generation(),
        problems.size(),
        String.join("\n  ", problems));
    return new ReloadResult(
        ReloadResult.REJECTED,
        generation(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        List.of(),
        problems);
  }

  private ReloadResult result(String result, ConfigDiff diff, List<String> retried) {
    List<ReloadResult.Restart> restarted = new ArrayList<>();
    diff.restarted()
        .forEach((name, reasons) -> restarted.add(new ReloadResult.Restart(name, reasons)));
    List<String> unchanged = new ArrayList<>(diff.unchanged());
    unchanged.removeAll(retried);
    return new ReloadResult(
        result,
        generation(),
        diff.added(),
        diff.removed(),
        restarted,
        retried,
        List.copyOf(unchanged),
        List.of());
  }

  // ---- watcher lifecycle ----

  @Override
  public void start() {
    running = true;
    KafkafimProperties.Reload settings = supervisor.properties().reload();
    if (!settings.enabled()) {
      LOG.info("Configuration watcher disabled (kafkafim.reload.enabled=false)");
      return;
    }
    if (watched.isEmpty()) {
      LOG.info("No file config locations to watch; reload with POST /actuator/reload");
      return;
    }
    ConfigWatcher created = watchers.apply(watched);
    if (created != null) {
      created.start();
      watcher = created;
    }
  }

  @Override
  public void stop() {
    running = false;
    ConfigWatcher current = watcher;
    watcher = null;
    if (current != null) {
      current.close();
    }
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }

  private ConfigWatcher defaultWatcher(List<Path> paths) {
    KafkafimProperties.Reload settings = supervisor.properties().reload();
    return new ConfigWatcher(
        paths, settings.pollInterval(), settings.debounce(), this::reloadFromWatcher);
  }

  private void reloadFromWatcher() {
    try {
      reload("watcher");
    } catch (IllegalStateException e) {
      LOG.info("Configuration change ignored: {}", e.getMessage());
    }
  }
}
