package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class KafkaPipelineSourceTest {

  @Test
  void concurrencyIsCappedAtThePartitionCount() {
    assertThat(KafkaPipelineSource.effectiveConcurrency(8, 3)).isEqualTo(3);
    assertThat(KafkaPipelineSource.effectiveConcurrency(2, 12)).isEqualTo(2);
    assertThat(KafkaPipelineSource.effectiveConcurrency(4, 4)).isEqualTo(4);
  }
}
