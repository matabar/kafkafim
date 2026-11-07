package io.github.matabar.kafkafim.config;

import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.cloud.CloudPlatform;
import org.springframework.context.EnvironmentAware;
import org.springframework.core.env.Environment;
import org.springframework.core.env.Profiles;

/**
 * Refuses to start with the {@code local} profile on Kubernetes (DESIGN §5.4, §5.6). A pod that
 * lost {@code SPRING_PROFILES_ACTIVE=kafkafim} would otherwise fall back to the default {@code
 * local} profile and silently run the compose sample with inline secrets allowed.
 *
 * <p>A {@link BeanFactoryPostProcessor} so it fails before any singleton (and therefore before any
 * client or configuration binding) is created. Kubernetes is {@link CloudPlatform#KUBERNETES} as
 * Spring Boot detects it ({@code KUBERNETES_SERVICE_HOST}/{@code _PORT}, overridable with {@code
 * spring.main.cloud-platform}).
 */
public final class LocalProfileGuard implements BeanFactoryPostProcessor, EnvironmentAware {

  /** The development profile that must never be active on Kubernetes. */
  public static final String LOCAL_PROFILE = "local";

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
   * Throws if {@code environment} runs the {@code local} profile on Kubernetes.
   *
   * @throws IllegalStateException with the remedy in the message
   */
  public static void check(Environment environment) {
    if (CloudPlatform.getActive(environment) == CloudPlatform.KUBERNETES
        && environment.acceptsProfiles(Profiles.of(LOCAL_PROFILE))) {
      throw new IllegalStateException(
          "The '"
              + LOCAL_PROFILE
              + "' profile (development sample, inline secrets allowed) is active on Kubernetes."
              + " Set SPRING_PROFILES_ACTIVE=kafkafim so the Helm-rendered"
              + " application-kafkafim.yaml is used instead (docs/DESIGN.md §5.4).");
    }
  }
}
