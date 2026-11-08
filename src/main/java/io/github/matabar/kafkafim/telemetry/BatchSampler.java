package io.github.matabar.kafkafim.telemetry;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.util.List;

/**
 * Head sampling of batches with errors always kept (DESIGN §10, ADR-0029): a root {@code
 * pipeline.batch} span that carries {@code error.type} is always sampled; every other root span
 * (other batches, HTTP server spans) is sampled by trace id at {@code
 * management.tracing.sampling.probability}; child spans follow their parent, so a batch's {@code
 * es.bulk} spans are kept exactly when the batch is. Replaces Spring Boot's default sampler, which
 * is the same without the error rule.
 */
public final class BatchSampler implements Sampler {

  private static final AttributeKey<String> ERROR_TYPE =
      AttributeKey.stringKey(BatchTracer.ERROR_TYPE);

  private final Sampler ratio;
  private final double probability;

  private BatchSampler(double probability) {
    this.probability = probability;
    this.ratio = Sampler.traceIdRatioBased(probability);
  }

  /**
   * The sampler to install.
   *
   * @param probability fraction of root spans without errors to keep, in {@code [0, 1]}
   */
  public static Sampler create(double probability) {
    return Sampler.parentBased(new BatchSampler(probability));
  }

  @Override
  public SamplingResult shouldSample(
      Context parentContext,
      String traceId,
      String name,
      SpanKind spanKind,
      Attributes attributes,
      List<LinkData> parentLinks) {
    if (BatchTracer.BATCH_SPAN.equals(name) && attributes.get(ERROR_TYPE) != null) {
      return SamplingResult.recordAndSample();
    }
    return ratio.shouldSample(parentContext, traceId, name, spanKind, attributes, parentLinks);
  }

  @Override
  public String getDescription() {
    return "KafkafimBatchSampler{errors=always,ratio=" + probability + "}";
  }
}
