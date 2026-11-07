package io.github.matabar.kafkafim.sink.es;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;

/** Captured Elasticsearch 8.19 responses under {@code src/test/resources/sink/es/}. */
final class Fixtures {

  private Fixtures() {}

  static byte[] load(String name) {
    try (InputStream in = Fixtures.class.getResourceAsStream("/sink/es/" + name)) {
      if (in == null) {
        throw new IllegalArgumentException("no fixture " + name);
      }
      return in.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
