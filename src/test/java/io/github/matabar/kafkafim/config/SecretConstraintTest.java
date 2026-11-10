package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.RecordComponent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * No Bean Validation constraint sits on a secret (DESIGN §5.5). Boot's {@code
 * BindValidationFailureAnalyzer} prints a {@code Value:} line for each failed constraint, and that
 * line is the raw value, not the record's masked {@code toString()}. Secret checks belong in {@code
 * ConfigValidator}, whose messages never include values.
 */
class SecretConstraintTest {

  @Test
  void noSecretComponentCarriesBeanValidationConstraints() {
    List<String> constrained = new ArrayList<>();
    for (Class<?> record : ConfigTestSupport.recordsReachableFrom(KafkafimProperties.class)) {
      constrained.addAll(constrainedSecrets(record));
    }
    assertThat(constrained).isEmpty();
  }

  @Test
  void theCheckFindsConstraintsOnSecrets() {
    // Guards the test above. Built-in constraints target FIELD, METHOD, PARAMETER and TYPE_USE at
    // once, so the single-target probes below show that each lookup is needed on its own.
    String probe = Probe.class.getName();
    assertThat(constrainedSecrets(Probe.class))
        .containsExactlyInAnyOrder(
            probe + ".password: @NotBlank",
            probe + ".apiKey: @Size",
            probe + ".properties: @NotBlank",
            probe + ".fieldPassword: @FieldOnly",
            probe + ".accessorPassword: @MethodOnly",
            probe + ".parameterPassword: @ParameterOnly");
  }

  /** Secret components of a record with a constraint, as {@code record.component: @Constraint}. */
  private static List<String> constrainedSecrets(Class<?> record) {
    List<String> found = new ArrayList<>();
    RecordComponent[] components = record.getRecordComponents();
    Class<?>[] types =
        Arrays.stream(components).map(RecordComponent::getType).toArray(Class[]::new);
    for (int i = 0; i < components.length; i++) {
      RecordComponent component = components[i];
      if (!ConfigTestSupport.holdsSecret(component)) {
        continue;
      }
      List<Annotation> annotations = new ArrayList<>();
      // A constraint on a component propagates to the field, the accessor and the constructor
      // parameter, depending on its @Target; type-use constraints sit on the (nested) type. No
      // Bean Validation constraint targets RECORD_COMPONENT, so the component itself has none.
      annotations.addAll(List.of(component.getAccessor().getAnnotations()));
      annotations.addAll(annotationsOf(() -> record.getDeclaredField(component.getName())));
      int position = i;
      annotations.addAll(
          annotationsOf(() -> record.getDeclaredConstructor(types).getParameters()[position]));
      collectTypeUse(component.getAnnotatedType(), annotations);
      annotations.stream()
          .filter(a -> a.annotationType().isAnnotationPresent(Constraint.class))
          .map(a -> "@" + a.annotationType().getSimpleName())
          .distinct()
          .forEach(a -> found.add(record.getName() + "." + component.getName() + ": " + a));
    }
    return found;
  }

  private static void collectTypeUse(AnnotatedType type, List<Annotation> annotations) {
    annotations.addAll(List.of(type.getAnnotations()));
    if (type instanceof AnnotatedParameterizedType parameterized) {
      for (AnnotatedType argument : parameterized.getAnnotatedActualTypeArguments()) {
        collectTypeUse(argument, annotations);
      }
    }
  }

  private static List<Annotation> annotationsOf(ElementLookup lookup) {
    try {
      return List.of(lookup.find().getAnnotations());
    } catch (ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  @FunctionalInterface
  private interface ElementLookup {
    AnnotatedElement find() throws ReflectiveOperationException;
  }

  /** A record that breaks the rule in each way the check must catch. */
  private record Probe(
      @NotBlank String password,
      @Size(min = 8) String apiKey,
      Map<String, @NotBlank String> properties,
      @FieldOnly String fieldPassword,
      @MethodOnly String accessorPassword,
      @ParameterOnly String parameterPassword,
      @NotEmpty String username,
      @Valid KafkaCluster.Sasl sasl) {}

  /** A constraint that propagates from a record component to the field only. */
  @Constraint(validatedBy = {})
  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface FieldOnly {
    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
  }

  /** A constraint that propagates from a record component to the accessor only. */
  @Constraint(validatedBy = {})
  @Target(ElementType.METHOD)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface MethodOnly {
    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
  }

  /** A constraint that propagates from a record component to the constructor parameter only. */
  @Constraint(validatedBy = {})
  @Target(ElementType.PARAMETER)
  @Retention(RetentionPolicy.RUNTIME)
  private @interface ParameterOnly {
    String message() default "";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
  }
}
