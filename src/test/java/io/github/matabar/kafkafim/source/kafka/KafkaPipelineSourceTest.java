package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

class KafkaPipelineSourceTest {

  @Test
  void concurrencyIsCappedAtThePartitionCount() {
    assertThat(KafkaPipelineSource.effectiveConcurrency(8, 3)).isEqualTo(3);
    assertThat(KafkaPipelineSource.effectiveConcurrency(2, 12)).isEqualTo(2);
    assertThat(KafkaPipelineSource.effectiveConcurrency(4, 4)).isEqualTo(4);
  }

  @Test
  void containerThatWasNeverStartedNeverRan() {
    ConcurrentMessageListenerContainer<byte[], byte[]> container =
        new ConcurrentMessageListenerContainer<>(
            new DefaultKafkaConsumerFactory<>(Map.of()), new ContainerProperties("topic"));

    assertThat(KafkaPipelineSource.neverRan(container)).isTrue();
  }
}
