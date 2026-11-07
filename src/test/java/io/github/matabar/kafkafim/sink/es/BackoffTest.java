package io.github.matabar.kafkafim.sink.es;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffTest {

  @Test
  void growsExponentiallyUpToTheCap() {
    Backoff backoff = new Backoff(Duration.ofMillis(100), Duration.ofSeconds(1), 2.0, 0.0);
    assertThat(backoff.delay(1)).isEqualTo(Duration.ofMillis(100));
    assertThat(backoff.delay(2)).isEqualTo(Duration.ofMillis(200));
    assertThat(backoff.delay(4)).isEqualTo(Duration.ofMillis(800));
    assertThat(backoff.delay(5)).isEqualTo(Duration.ofSeconds(1));
    assertThat(backoff.delay(Integer.MAX_VALUE)).isEqualTo(Duration.ofSeconds(1));
  }

  @Test
  void jitterStaysWithinBoundsAndTheCap() {
    for (int attempt = 1; attempt < 1000; attempt++) {
      Duration delay = Backoff.DEFAULT.delay(attempt);
      assertThat(delay).isPositive().isLessThanOrEqualTo(Duration.ofSeconds(30));
    }
    assertThat(Backoff.DEFAULT.delay(1)).isBetween(Duration.ofMillis(50), Duration.ofMillis(150));
  }

  @Test
  void rejectsNonsense() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Backoff(Duration.ZERO, Duration.ofSeconds(1), 2.0, 0.0));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Backoff(Duration.ofSeconds(2), Duration.ofSeconds(1), 2.0, 0.0));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(1), 0.5, 0.0));
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new Backoff(Duration.ofSeconds(1), Duration.ofSeconds(1), 2.0, 1.0));
  }
}
