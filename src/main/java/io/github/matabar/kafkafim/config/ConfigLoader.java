package io.github.matabar.kafkafim.config;

import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.bind.BindException;
import org.springframework.boot.context.properties.bind.BindHandler;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.bind.handler.IgnoreTopLevelConverterNotFoundBindHandler;
import org.springframework.boot.context.properties.bind.handler.NoUnboundElementsBindHandler;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.context.properties.bind.validation.ValidationBindHandler;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.UnboundElementsSourceFilter;
import org.springframework.boot.convert.ApplicationConversionService;
import org.springframework.core.env.Environment;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.springframework.validation.beanvalidation.SpringValidatorAdapter;

/**
 * Binds and validates {@code kafkafim.*} from an {@link Environment} the way startup does, for a
 * hot reload (DESIGN §5.6): the Kubernetes guard of {@link LocalProfileGuard}, then Spring's {@link
 * Binder} with the bind handlers {@code @ConfigurationProperties(ignoreUnknownFields = false)} gets
 * (unknown keys fail), Bean Validation and {@link KafkafimPropertiesValidator} (cross-reference
 * rules and the inline-secret check against this environment's own property sources) in one pass. A
 * tree is either valid as a whole or rejected with every violation.
 */
public final class ConfigLoader {

  private static final ValidatorFactory BEAN_VALIDATION =
      Validation.byDefaultProvider().configure().buildValidatorFactory();

  private ConfigLoader() {}

  /**
   * What a load produced.
   *
   * @param properties the bound tree, or {@code null} when it is invalid
   * @param problems every violation as {@code <property path>: <problem>}; never contains a secret
   *     value
   */
  public record Result(@Nullable KafkafimProperties properties, List<String> problems) {

    /** Normalizes the problem list. */
    public Result {
      problems = List.copyOf(problems);
    }

    /** Whether the tree bound and passed every check. */
    public boolean isValid() {
      return properties != null && problems.isEmpty();
    }
  }

  /**
   * Binds and validates {@code kafkafim.*} from {@code environment}; never throws for an invalid
   * tree.
   *
   * <p>Attaches Boot's {@code configurationProperties} source to {@code environment} when it is not
   * attached yet, as {@code SpringApplication} does at startup: placeholders then resolve through
   * relaxed names exactly as they do there, which is the resolution the inline-secret check
   * mirrors.
   *
   * @param environment a fully loaded environment (config data processed); must be a {@code
   *     ConfigurableEnvironment}
   * @return the tree, or every problem found
   * @throws IllegalArgumentException if {@code environment} is not configurable
   */
  public static Result load(Environment environment) {
    try {
      LocalProfileGuard.check(environment);
    } catch (IllegalStateException e) {
      return new Result(null, List.of(e.getMessage()));
    }
    ConfigurationPropertySources.attach(environment);
    Iterable<ConfigurationPropertySource> sources = ConfigurationPropertySources.get(environment);
    Binder binder =
        new Binder(
            sources,
            new PropertySourcesPlaceholdersResolver(environment),
            ApplicationConversionService.getSharedInstance());
    BindHandler handler =
        new ValidationBindHandler(
            new NoUnboundElementsBindHandler(
                new IgnoreTopLevelConverterNotFoundBindHandler(),
                new UnboundElementsSourceFilter()),
            new SpringValidatorAdapter(BEAN_VALIDATION.getValidator()),
            new KafkafimPropertiesValidator(() -> sources));
    try {
      KafkafimProperties properties =
          binder.bindOrCreate(
              KafkafimProperties.PREFIX, Bindable.of(KafkafimProperties.class), handler);
      return new Result(properties, List.of());
    } catch (BindException e) {
      return new Result(null, problems(e));
    }
  }

  private static List<String> problems(BindException failure) {
    for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
      if (t instanceof BindValidationException validation) {
        List<String> problems = new ArrayList<>();
        for (ObjectError error : validation.getValidationErrors().getAllErrors()) {
          problems.add(describe(error));
        }
        return problems;
      }
    }
    // A conversion failure or unknown keys: name the property, then the innermost cause.
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    String detail = root == failure ? "" : ": " + root.getMessage();
    return List.of(failure.getName() + ": " + failure.getMessage() + detail);
  }

  private static String describe(ObjectError error) {
    if (error instanceof FieldError field) {
      return field.getField() + ": " + field.getDefaultMessage();
    }
    return Objects.requireNonNullElse(error.getDefaultMessage(), error.toString());
  }
}
