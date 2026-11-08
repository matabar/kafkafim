package io.github.matabar.kafkafim.supervisor;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.matabar.kafkafim.dlq.DlqListener;
import io.github.matabar.kafkafim.sink.es.SinkListener;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/** Every listener event reaches the telemetry listeners, including events added later. */
class ListenerRelayTest {

  private final List<String> received = new CopyOnWriteArrayList<>();

  @SuppressWarnings("unchecked")
  private <L> L recorder(Class<L> type) {
    return (L)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return method.getName().equals("toString") ? "recorder" : null;
              }
              received.add(method.getName());
              return null;
            });
  }

  private static Set<String> events(Class<?> type) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(m -> !Modifier.isStatic(m.getModifiers()))
        .map(Method::getName)
        .collect(Collectors.toSet());
  }

  private static void fireAll(Object listener, Class<?> type) throws Exception {
    for (Method method : type.getDeclaredMethods()) {
      if (Modifier.isStatic(method.getModifiers())) {
        continue;
      }
      Object[] args = new Object[method.getParameterCount()];
      Class<?>[] types = method.getParameterTypes();
      for (int i = 0; i < args.length; i++) {
        args[i] = zero(types[i]);
      }
      try {
        method.invoke(listener, args);
      } catch (java.lang.reflect.InvocationTargetException e) {
        // a null argument may trip the supervisor side; forwarding still happened or not
      }
    }
  }

  private static Object zero(Class<?> type) {
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    return null;
  }

  @Test
  void everySinkAndDlqEventIsForwarded() throws Exception {
    ListenerRelay relay =
        new ListenerRelay(
            List.of(recorder(SinkListener.class)), List.of(recorder(DlqListener.class)));

    fireAll(relay.sinkListener(() -> new Object()), SinkListener.class);
    fireAll(relay.dlqListener(() -> new Object()), DlqListener.class);

    assertThat(received)
        .containsAll(events(SinkListener.class))
        .containsAll(events(DlqListener.class));
  }
}
