package io.github.matabar.kafkafim.source.kafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ByteBudgetTest {

  private final List<Boolean> events = new ArrayList<>();
  private final ByteBudget budget = new ByteBudget(100, 50, events::add);

  @Test
  void pausesAtTheBudgetAndResumesBelowTheThreshold() {
    budget.acquire(60);
    budget.acquire(39);
    assertThat(events).isEmpty();

    budget.acquire(1);
    assertThat(events).containsExactly(true);
    assertThat(budget.isExhausted()).isTrue();

    budget.acquire(500);
    assertThat(events).as("one pause request per crossing").containsExactly(true);

    budget.release(500);
    budget.release(49);
    assertThat(events).as("51 outstanding: still above the resume threshold").hasSize(1);

    budget.release(1);
    assertThat(events).containsExactly(true, false);
    assertThat(budget.outstanding()).isEqualTo(50);

    budget.acquire(49);
    assertThat(events).as("between thresholds after a resume: no pause").hasSize(2);
    budget.acquire(1);
    assertThat(events).containsExactly(true, false, true);
  }

  @Test
  void releasingWhileNotExhaustedSendsNothing() {
    budget.acquire(10);
    budget.release(10);
    assertThat(events).isEmpty();
  }

  @Test
  void rejectsInvalidThresholds() {
    assertThatThrownBy(() -> new ByteBudget(0, 0, e -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ByteBudget(10, 10, e -> {}))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new ByteBudget(10, -1, e -> {}))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
