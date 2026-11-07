package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PauseReasonsTest {

  private final List<String> calls = new ArrayList<>();
  private final PauseReasons reasons =
      new PauseReasons(
          new PauseReasons.Target() {
            @Override
            public void pause() {
              calls.add("pause");
            }

            @Override
            public void resume() {
              calls.add("resume");
            }
          });

  @Test
  void pausedWhileAnyReasonIsActive() {
    assertThat(reasons.add("budget")).isTrue();
    assertThat(reasons.add("breaker:search-eu")).isTrue();
    assertThat(reasons.add("budget")).isFalse();
    assertThat(calls).containsExactly("pause");

    assertThat(reasons.remove("budget")).isTrue();
    assertThat(calls).as("breaker still active").containsExactly("pause");
    assertThat(reasons.active()).containsExactly("breaker:search-eu");

    assertThat(reasons.remove("breaker:search-eu")).isTrue();
    assertThat(calls).containsExactly("pause", "resume");
    assertThat(reasons.remove("breaker:search-eu")).isFalse();
    assertThat(calls).hasSize(2);
  }

  @Test
  void applyReplaysAnActivePause() {
    reasons.apply();
    assertThat(calls).isEmpty();
    reasons.add("auth");
    reasons.apply();
    assertThat(calls).containsExactly("pause", "pause");
  }
}
