package io.github.matabar.kafkafim.supervisor;

import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.KafkafimProperties;
import io.github.matabar.kafkafim.config.Pipeline;
import io.github.matabar.kafkafim.decode.DecodeSpec;
import io.github.matabar.kafkafim.decode.Decoder;
import io.github.matabar.kafkafim.route.ConditionSpec;
import io.github.matabar.kafkafim.route.RouteMode;
import io.github.matabar.kafkafim.route.RouteSpec;
import io.github.matabar.kafkafim.route.Router;
import io.github.matabar.kafkafim.sink.es.SinkSettings;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Maps the bound configuration of one pipeline onto the specs of the building blocks: {@link
 * DecodeSpec}, {@link RouteSpec}/{@link ConditionSpec}, and one {@link SinkSettings} per bulk
 * stream (DESIGN §4.2, §5.2, §5.3). Pure functions over a validated model; anything the building
 * blocks would reject (or that would make Elasticsearch reject a whole bulk request) is reported as
 * {@link IllegalArgumentException}, which the supervisor turns into {@code failed_preflight}.
 */
public final class PipelineMapping {

  private PipelineMapping() {}

  /**
   * Compiles one pipeline.
   *
   * @param name pipeline name
   * @param properties the whole validated configuration (for the Elasticsearch clusters)
   * @return the compiled pipeline
   * @throws IllegalArgumentException if the pipeline cannot run as configured
   */
  public static CompiledPipeline compile(String name, KafkafimProperties properties) {
    Pipeline pipeline = properties.pipelines().get(name);
    if (pipeline == null) {
      throw new IllegalArgumentException("Unknown pipeline '" + name + "'");
    }
    Pipeline.Document document = pipeline.document();
    if (document.op() == Pipeline.Op.CREATE && document.versionField() != null) {
      // Elasticsearch rejects external versions on create for the whole bulk request (DESIGN §7).
      throw new IllegalArgumentException(
          "document.op 'create' cannot be combined with document.version-field");
    }
    Decoder decoder = Decoder.compile(decodeSpec(pipeline));
    List<RouteSpec<RouteTarget>> routes = routeSpecs(pipeline);
    Router<RouteTarget> router = Router.compile(routeMode(pipeline.routeMode()), routes);
    Map<RouteTarget.StreamKey, SinkSettings> streams = new LinkedHashMap<>();
    for (RouteSpec<RouteTarget> route : routes) {
      RouteTarget target = route.target();
      ElasticsearchCluster cluster = properties.elasticsearch().clusters().get(target.cluster());
      if (cluster == null) {
        throw new IllegalArgumentException(
            "route "
                + target.label()
                + ": unknown Elasticsearch cluster '"
                + target.cluster()
                + "'");
      }
      streams.computeIfAbsent(target.stream(), k -> sinkSettings(cluster, pipeline));
    }
    return new CompiledPipeline(
        name, pipeline, decoder, router, streams, document.op(), document.timestampField() != null);
  }

  /** {@code document} plus {@code source.max-record-bytes} as the decoder's spec. */
  static DecodeSpec decodeSpec(Pipeline pipeline) {
    Pipeline.Document document = pipeline.document();
    return new DecodeSpec(
        document.idField(),
        switch (document.idMissing()) {
          case DLQ -> DecodeSpec.IdMissing.DLQ;
          case AUTO -> DecodeSpec.IdMissing.AUTO;
        },
        document.versionField(),
        document.timestampField(),
        document.timestampFormat(),
        switch (document.onBadTimestamp()) {
          case DLQ -> DecodeSpec.OnBadTimestamp.DLQ;
          case INGEST_TIME -> DecodeSpec.OnBadTimestamp.INGEST_TIME;
        },
        pipeline.source().maxRecordBytes().toBytes());
  }

  /** {@code route-mode} as the router's mode. */
  static RouteMode routeMode(Pipeline.RouteMode mode) {
    return switch (mode) {
      case FIRST -> RouteMode.FIRST;
      case ALL -> RouteMode.ALL;
    };
  }

  /** The routes, in configuration order, each with its {@link RouteTarget}. */
  static List<RouteSpec<RouteTarget>> routeSpecs(Pipeline pipeline) {
    List<RouteSpec<RouteTarget>> specs = new ArrayList<>(pipeline.routes().size());
    for (int i = 0; i < pipeline.routes().size(); i++) {
      Pipeline.Route route = pipeline.routes().get(i);
      String label = route.name() != null ? route.name() : Integer.toString(i);
      if (route.pipeline() != null && route.pipeline().isBlank()) {
        throw new IllegalArgumentException("route " + label + ": ingest pipeline name is blank");
      }
      List<ConditionSpec> when = new ArrayList<>(route.when().size());
      for (Pipeline.Condition condition : route.when()) {
        when.add(condition(label, condition));
      }
      specs.add(
          new RouteSpec<>(
              new RouteTarget(route.cluster(), route.identity(), route.pipeline(), label),
              when,
              route.isDefault(),
              route.index()));
    }
    return specs;
  }

  /** One {@code when} entry as a condition spec. */
  static ConditionSpec condition(String routeLabel, Pipeline.Condition c) {
    if (c.topic() != null) {
      return new ConditionSpec.TopicEquals(c.topic());
    }
    if (c.header() != null) {
      if (c.equalTo() == null) {
        throw new IllegalArgumentException(
            "route " + routeLabel + ": header condition '" + c.header() + "' needs equals");
      }
      return new ConditionSpec.HeaderEquals(c.header(), c.equalTo());
    }
    if (c.field() != null) {
      if (c.equalTo() != null) {
        return new ConditionSpec.FieldEquals(c.field(), c.equalTo());
      }
      if (c.oneOf() != null) {
        return new ConditionSpec.FieldIn(c.field(), c.oneOf());
      }
      if (c.exists() != null) {
        return new ConditionSpec.FieldExists(c.field(), c.exists());
      }
      if (c.prefix() != null) {
        return new ConditionSpec.FieldPrefix(c.field(), c.prefix());
      }
      if (c.matches() != null) {
        return new ConditionSpec.FieldMatches(c.field(), c.matches());
      }
    }
    throw new IllegalArgumentException(
        "route " + routeLabel + ": condition has no usable subject and operator");
  }

  /**
   * The cluster's bulk limits with the pipeline's {@code latency.max-age} override, poison bound
   * and version-conflict policy.
   */
  static SinkSettings sinkSettings(ElasticsearchCluster cluster, Pipeline pipeline) {
    return SinkSettings.of(
        cluster.bulk(),
        pipeline.latency().maxAge(),
        pipeline.dlq(),
        pipeline.document().onVersionConflict());
  }
}
