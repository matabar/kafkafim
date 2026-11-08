package io.github.matabar.kafkafim.supervisor;

import java.util.Arrays;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.ConfigTreePropertySource;
import org.springframework.boot.env.OriginTrackedMapPropertySource;
import org.springframework.core.env.AbstractEnvironment;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.DefaultResourceLoader;

/**
 * Re-reads the application's config data from disk for a reload (DESIGN §5.6), with the precedence
 * startup had.
 *
 * <p>A new environment gets every property source of the running one that did <em>not</em> come
 * from config data, in the same order: command-line arguments, system properties, environment
 * variables, {@code SpringApplication} default properties, test properties. Then Spring Boot's own
 * {@link ConfigDataEnvironmentPostProcessor} runs on it, exactly as at startup: it reads {@code
 * spring.config.location}, {@code spring.config.additional-location} and {@code
 * spring.config.import} from those sources, loads {@code application.yaml}, the profile-specific
 * files of the profiles active in the running application and every import (including {@code
 * configtree:} secret directories, re-read file by file), and places them where startup placed
 * them. Environment variables and system properties therefore still override files, and files still
 * override {@code SpringApplication} defaults.
 *
 * <p>The active profiles are pinned to the running ones: profiles are chosen at startup (from
 * environment variables, which cannot change in a running process) and a reload never switches
 * them.
 */
final class FreshEnvironment {

  /** Prefix Boot gives the property sources of config files ({@code StandardConfigDataLoader}). */
  private static final String CONFIG_RESOURCE_PREFIX = "Config resource '";

  private FreshEnvironment() {}

  /**
   * Loads the config data of {@code running}'s locations again.
   *
   * @param running the application's environment
   * @return a new environment; {@code running} is not modified
   * @throws RuntimeException if a location cannot be read or parsed (a non-optional location
   *     missing, malformed YAML)
   */
  static ConfigurableEnvironment load(ConfigurableEnvironment running) {
    ConfigurableEnvironment fresh = new AbstractEnvironment() {};
    MutablePropertySources sources = fresh.getPropertySources();
    for (PropertySource<?> source : running.getPropertySources()) {
      if (!isConfigData(source)
          && !ConfigurationPropertySources.isAttachedConfigurationPropertySource(source)) {
        sources.addLast(source);
      }
    }
    fresh.setConversionService(running.getConversionService());
    ConfigDataEnvironmentPostProcessor.applyTo(
        fresh,
        new DefaultResourceLoader(FreshEnvironment.class.getClassLoader()),
        null,
        Arrays.asList(running.getActiveProfiles()));
    ConfigurationPropertySources.attach(fresh);
    return fresh;
  }

  /** Whether a property source was loaded from config data (a file or a config tree). */
  static boolean isConfigData(PropertySource<?> source) {
    return source instanceof ConfigTreePropertySource
        || source instanceof OriginTrackedMapPropertySource
            && source.getName().startsWith(CONFIG_RESOURCE_PREFIX);
  }
}
