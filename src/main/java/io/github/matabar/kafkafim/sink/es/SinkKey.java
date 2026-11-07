package io.github.matabar.kafkafim.sink.es;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Identifies one bulk stream: a pipeline writing to one Elasticsearch cluster under one identity
 * (DESIGN §4.2, §5.2.1). Names only, never credentials, so it is safe in logs and metric labels.
 *
 * @param pipeline kafkafim pipeline name
 * @param cluster Elasticsearch cluster name
 * @param identity identity name, or {@code null} for an unauthenticated {@code http} cluster
 */
public record SinkKey(String pipeline, String cluster, @Nullable String identity) {

  /** Requires the pipeline and cluster names. */
  public SinkKey {
    Objects.requireNonNull(pipeline, "pipeline");
    Objects.requireNonNull(cluster, "cluster");
  }

  @Override
  public String toString() {
    return pipeline + "→" + cluster + "/" + (identity == null ? "<none>" : identity);
  }
}
