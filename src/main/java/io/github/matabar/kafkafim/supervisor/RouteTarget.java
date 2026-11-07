package io.github.matabar.kafkafim.supervisor;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Where a selected route sends a document: the {@code route} package's opaque target for one
 * configured route.
 *
 * @param cluster Elasticsearch cluster name
 * @param identity resolved Elasticsearch identity, or {@code null} for an unauthenticated {@code
 *     http} cluster
 * @param ingestPipeline Elasticsearch ingest pipeline named on the route, or {@code null}
 * @param label the route's {@code name}, or its zero-based position; for logs and metrics, never a
 *     resolved index
 */
public record RouteTarget(
    String cluster, @Nullable String identity, @Nullable String ingestPipeline, String label) {

  /** Requires the cluster and label. */
  public RouteTarget {
    Objects.requireNonNull(cluster, "cluster");
    Objects.requireNonNull(label, "label");
  }

  /** The bulk stream this target writes to: one per {@code (cluster, identity)}. */
  public StreamKey stream() {
    return new StreamKey(cluster, identity);
  }

  /**
   * One {@code (cluster, identity)} pair; a pipeline opens one bulk sink per pair (DESIGN §5.2.1).
   *
   * @param cluster Elasticsearch cluster name
   * @param identity identity name, or {@code null}
   */
  public record StreamKey(String cluster, @Nullable String identity) {

    /** Requires the cluster. */
    public StreamKey {
      Objects.requireNonNull(cluster, "cluster");
    }

    @Override
    public String toString() {
      return cluster + "/" + (identity == null ? "<none>" : identity);
    }
  }
}
