package io.github.matabar.kafkafim.telemetry;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class TraceParentTest {

  @Test
  void parsesTheW3cExample() {
    assertThat(TraceParent.parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"))
        .isEqualTo(new TraceParent("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", true));
    assertThat(TraceParent.parse(" 00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-00 "))
        .isEqualTo(new TraceParent("4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7", false));
  }

  @Test
  void acceptsLaterVersionsWithMoreFields() {
    assertThat(TraceParent.parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra"))
        .isNotNull();
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(
      strings = {
        "",
        "garbage",
        "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
        "00-4bf92f3577b34da6a3ce929d0e0e473x-00f067aa0ba902b7-01",
        "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
        "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
      })
  void rejectsInvalidValues(String value) {
    assertThat(TraceParent.parse(value)).isNull();
  }
}
