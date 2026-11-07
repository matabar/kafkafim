package io.github.matabar.kafkafim;

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
  static final ArchRule sinkIsIndependentOfSourceDlqAndSupervisor =
      noClasses()
          .that()
          .resideInAPackage(SINK)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(SOURCE, DLQ, SUPERVISOR)
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule sourceIsIndependentOfSinkDlqAndSupervisor =
      noClasses()
          .that()
          .resideInAPackage(SOURCE)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(SINK, DLQ, SUPERVISOR)
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule dlqIsIndependentOfSinkSourceAndSupervisor =
      noClasses()
          .that()
          .resideInAPackage(DLQ)
          .should()
          .dependOnClassesThat()
          .resideInAnyPackage(SINK, SOURCE, SUPERVISOR)
          .allowEmptyShould(true);

  @ArchTest
  static final ArchRule topLevelPackagesAreFreeOfCycles =
      slices().matching(ROOT + ".(*)..").should().beFreeOfCycles().allowEmptyShould(true);

  private static String[] concat(String[]... groups) {
    return Arrays.stream(groups).flatMap(Arrays::stream).toArray(String[]::new);
  }
}
