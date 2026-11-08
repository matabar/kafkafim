package io.github.matabar.kafkafim;

import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage;
import static com.tngtech.archunit.core.domain.JavaClass.Predicates.resideOutsideOfPackages;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;
import static com.tngtech.archunit.library.dependencies.SlicesRuleDefinition.slices;

import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.junit.AnalyzeClasses;
import com.tngtech.archunit.junit.ArchTest;
import com.tngtech.archunit.lang.ArchRule;
import java.util.Arrays;

/**
 * Package boundaries (ADR-0003). Every rule allows empty packages so that the slices which create
 * {@code route}, {@code decode}, {@code sink}, {@code source}, {@code dlq} and {@code supervisor}
 * inherit them unchanged.
 */
@AnalyzeClasses(
    packages = ArchitectureTest.ROOT,
    importOptions = ImportOption.DoNotIncludeTests.class)
class ArchitectureTest {

  static final String ROOT = "io.github.matabar.kafkafim";

  private static final String CONFIG = ROOT + ".config..";
  private static final String ROUTE = ROOT + ".route..";
  private static final String DECODE = ROOT + ".decode..";
  private static final String SINK = ROOT + ".sink..";
  private static final String SOURCE = ROOT + ".source..";
  private static final String DLQ = ROOT + ".dlq..";
  private static final String TELEMETRY = ROOT + ".telemetry..";
  private static final String SUPERVISOR = ROOT + ".supervisor..";

  /** The JDK and Jackson (data path), plus nullness annotations. */
  private static final String[] JDK_AND_JACKSON = {
    "java..", "tools.jackson..", "com.fasterxml.jackson.annotation..", "org.jspecify.."
  };

  /**
   * What the configuration model needs beyond the JDK and Jackson: Bean Validation, Spring Boot's
   * binding and property-source types, {@code DataSize}, Spring's {@code Validator}, the bean
   * definitions that register the validator and the profile guard, and logging.
   */
  private static final String[] CONFIG_SPRING = {
    "jakarta.validation..",
    "org.springframework.boot.context.properties..",
    "org.springframework.boot.convert..",
    "org.springframework.boot.env..",
    "org.springframework.boot.cloud..",
    "org.springframework.boot.origin..",
    "org.springframework.util.unit..",
    "org.springframework.validation..",
    "org.springframework.core.env..",
    "org.springframework.context",
    "org.springframework.context.annotation..",
    "org.springframework.beans.factory.config..",
    "org.slf4j.."
  };

  @ArchTest
  static final ArchRule configDependsOnlyOnJdkJacksonAndSpringConfiguration =
      classes()
          .that()
          .resideInAPackage(CONFIG)
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(concat(new String[] {CONFIG}, JDK_AND_JACKSON, CONFIG_SPRING))
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule routeDependsOnlyOnJdkAndJackson =
      classes()
          .that()
          .resideInAPackage(ROUTE)
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(concat(new String[] {ROUTE}, JDK_AND_JACKSON))
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule decodeDependsOnlyOnJdkAndJackson =
      classes()
          .that()
          .resideInAPackage(DECODE)
          .should()
          .onlyDependOnClassesThat()
          .resideInAnyPackage(concat(new String[] {DECODE}, JDK_AND_JACKSON))
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule sinkDependsOnlyOnConfigAndTelemetry = onlyConfigAndTelemetry(SINK);

  @ArchTest
  static final ArchRule sourceDependsOnlyOnConfigAndTelemetry = onlyConfigAndTelemetry(SOURCE);

  @ArchTest static final ArchRule dlqDependsOnlyOnConfigAndTelemetry = onlyConfigAndTelemetry(DLQ);

  /**
   * {@code telemetry} is an inner package (ADR-0003): it uses no kafkafim package but {@code
   * config}, so {@code sink}, {@code source} and {@code dlq} could use it, and the supervisor
   * adapts the building blocks' listener interfaces to it. Third-party libraries are unrestricted.
   */
  @ArchTest
  static final ArchRule telemetryDependsOnlyOnConfig =
      noClasses()
          .that()
          .resideInAPackage(TELEMETRY)
          .should()
          .dependOnClassesThat(
              resideInAPackage(ROOT + "..").and(resideOutsideOfPackages(TELEMETRY, CONFIG)))
          .allowEmptyShould(true);

  /** The supervisor wires everything; nothing depends on it (ADR-0003). */
  @ArchTest
  static final ArchRule nothingDependsOnSupervisor =
      noClasses()
          .that()
          .resideOutsideOfPackage(SUPERVISOR)
          .should()
          .dependOnClassesThat()
          .resideInAPackage(SUPERVISOR)
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule topLevelPackagesAreFreeOfCycles =
      slices().matching(ROOT + ".(*)..").should().beFreeOfCycles().allowEmptyShould(true);

  /**
   * ADR-0003: {@code sink.es}, {@code source.kafka} and {@code dlq} use no kafkafim package but
   * their own, {@code config} and {@code telemetry} (so not each other, {@code supervisor}, {@code
   * route} or {@code decode}). Third-party libraries are unrestricted.
   */
  private static ArchRule onlyConfigAndTelemetry(String layer) {
    return noClasses()
        .that()
        .resideInAPackage(layer)
        .should()
        .dependOnClassesThat(
            resideInAPackage(ROOT + "..").and(resideOutsideOfPackages(layer, CONFIG, TELEMETRY)))
        .allowEmptyShould(true);
  }

  private static String[] concat(String[]... groups) {
    return Arrays.stream(groups).flatMap(Arrays::stream).toArray(String[]::new);
  }
}
