package io.github.matabar.kafkafim.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.cloud.CloudPlatform;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/**
 * Refuses development settings on Kubernetes (DESIGN §5.4, §5.5, §5.6):
 *
 * <ul>
 *   <li>the {@code local} profile, as the default or explicitly. A pod that lost {@code
 *       SPRING_PROFILES_ACTIVE=kafkafim} would otherwise fall back to the default {@code local}
 *       profile and silently run the compose sample with inline secrets allowed;
 *   <li>{@code kafkafim.allow-inline-secrets=true} in any profile. On Kubernetes secrets come from
 *       mounted {@code Secret} files imported with {@code configtree}, never from literal values.
 * </ul>
 *
 * <p>A {@link BeanFactoryPostProcessor} so it fails before any singleton (and therefore before any
 * client or configuration binding) is created. Kubernetes is {@link CloudPlatform#KUBERNETES} as
 * Spring Boot detects it ({@code KUBERNETES_SERVICE_HOST}/{@code _PORT}, overridable with {@code
 * spring.main.cloud-platform}).
 */
public final class LocalProfileGuard implements BeanFactoryPostProcessor, EnvironmentAware {

  /** The development profile that must never be active on Kubernetes. */
  public static final String LOCAL_PROFILE = "local";

  /** The development flag that must never be set on Kubernetes. */
  public static final String ALLOW_INLINE_SECRETS =
      KafkafimProperties.PREFIX + ".allow-inline-secrets";

  private Environment environment;

  @Override
  public void setEnvironment(Environment environment) {
    this.environment = environment;
  }

  @Override
  public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) {
    check(environment);
  }

  /**
   * Throws if {@code environment} runs the {@code local} profile or allows inline secrets on
   * Kubernetes.
   *
   * @throws IllegalStateException with the remedy in the message
   */
  public static void check(Environment environment) {
    if (CloudPlatform.getActive(environment) != CloudPlatform.KUBERNETES) {
      return;
    }
    if (environment.acceptsProfiles(Profiles.of(LOCAL_PROFILE))) {
      throw new IllegalStateException(
          "The '"
              + LOCAL_PROFILE
              + "' profile (development sample, inline secrets allowed) is active on Kubernetes."
              + " Set SPRING_PROFILES_ACTIVE=kafkafim so the Helm-rendered"
              + " application-kafkafim.yaml is used instead (docs/DESIGN.md §5.4).");
    }
    // The Binder applies relaxed binding, so KAFKAFIM_ALLOWINLINESECRETS counts too.
    if (Binder.get(environment).bind(ALLOW_INLINE_SECRETS, Boolean.class).orElse(false)) {
      throw new IllegalStateException(
          ALLOW_INLINE_SECRETS
              + "=true is set on Kubernetes. Secrets must come from mounted Secret files"
              + " imported with configtree (spring.config.import=configtree:...) and referenced"
              + " with ${...} placeholders; remove the setting (docs/DESIGN.md §5.5).");
    }
  }
}
