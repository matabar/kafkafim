package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.KafkafimProperties;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ByteArrayResource;

/** Binds {@code kafkafim.*} YAML into the configuration model, as startup does (no validation). */
final class TestConfigs {

  /** Kafka cluster {@code k} and Elasticsearch clusters {@code es} and {@code es2}. */
  static final String CLUSTERS =
      """
      kafkafim:
        kafka:
          clusters:
            k: { bootstrap-servers: [k1:9092] }
        elasticsearch:
          clusters:
            es:
              uris: [http://es:9200]
              identities:
                writer: { username: u, password: p }
              default-identity: writer
              bulk: { max-docs: 10, max-bytes: 1MB, max-age: 300ms, in-flight: 2 }
            es2:
              uris: [http://es2:9200]
      """;

  private TestConfigs() {}

  /** {@link #CLUSTERS} plus {@code pipelines:} with the given body (no base indentation). */
  static KafkafimProperties withPipelines(String pipelines) {
    return bind(CLUSTERS + "  pipelines:\n" + pipelines.indent(4));
  }

  static KafkafimProperties bind(String yaml) {
    try {
      List<PropertySource<?>> sources =
          new YamlPropertySourceLoader()
              .load("test", new ByteArrayResource(yaml.getBytes(StandardCharsets.UTF_8)));
      return new Binder(ConfigurationPropertySources.from(sources))
          .bind(KafkafimProperties.PREFIX, KafkafimProperties.class)
          .get();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
