package io.github.matabar.kafkafim.config;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

/**
 * Spring {@link Validator} that runs {@link ConfigValidator} and {@link InlineSecretCheck} on the
 * bound {@link KafkafimProperties}. Registered as Boot's {@code configurationPropertiesValidator},
 * it runs inside the same {@code ValidationBindHandler} as Bean Validation, so Bean Validation and
 * cross-reference errors are reported together in one {@code BindValidationException}. Warnings are
 * logged once per bind.
 */
public final class KafkafimPropertiesValidator implements Validator {

  /** Error code of every cross-reference violation. */
  public static final String ERROR_CODE = "kafkafim.invalid";

  private static final Logger LOG = LoggerFactory.getLogger(KafkafimPropertiesValidator.class);

  private final Supplier<Iterable<ConfigurationPropertySource>> sources;
  private final boolean allowUnmountedSecrets;

  /**
   * Creates a validator.
   *
   * @param sources the property sources the tree was bound from, consulted for raw (unresolved)
   *     secret values
   * @param allowUnmountedSecrets accept credential placeholders that nothing defines; {@code true}
   *     only under the {@code validate} profile ({@link ValidateProfile#isActive})
   */
  public KafkafimPropertiesValidator(
      Supplier<Iterable<ConfigurationPropertySource>> sources, boolean allowUnmountedSecrets) {
    this.sources = sources;
    this.allowUnmountedSecrets = allowUnmountedSecrets;
  }

  @Override
  public boolean supports(Class<?> type) {
    return KafkafimProperties.class.isAssignableFrom(type);
  }

  @Override
  public void validate(Object target, Errors errors) {
    for (String problem : problems((KafkafimProperties) target)) {
      errors.reject(ERROR_CODE, problem);
    }
  }

  /** Every violation in {@code properties}; logs the warnings. */
  public List<String> problems(KafkafimProperties properties) {
    ConfigValidator.Result result = ConfigValidator.validate(properties);
    result.warnings().forEach(warning -> LOG.warn("kafkafim configuration: {}", warning));
    List<String> problems = new ArrayList<>(result.errors());
    problems.addAll(
        InlineSecretCheck.errors(
            properties, sources.get(), properties.allowInlineSecrets(), allowUnmountedSecrets));
    return problems;
  }
}
