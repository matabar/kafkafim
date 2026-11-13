package io.github.matabar.kafkafim.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.Role;
import org.springframework.core.env.Environment;

/**
 * Binds and validates {@code kafkafim.*}, guards the profile choice at startup and, under the
 * {@code validate} profile, logs the summary.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkafimProperties.class)
public class KafkafimConfiguration {

  private static final Logger LOG = LoggerFactory.getLogger(KafkafimConfiguration.class);

  /**
   * Boot applies the bean named {@code configurationPropertiesValidator} to every
   * {@code @ConfigurationProperties} binding; this one only {@link
   * KafkafimPropertiesValidator#supports supports} {@link KafkafimProperties}. Static because the
   * binder looks it up before regular beans exist; infrastructure role because it is created while
   * bean post-processors are still being registered.
   */
  @Bean
  @Role(BeanDefinition.ROLE_INFRASTRUCTURE)
  static KafkafimPropertiesValidator configurationPropertiesValidator(Environment environment) {
    return new KafkafimPropertiesValidator(
        () -> ConfigurationPropertySources.get(environment), ValidateProfile.isActive(environment));
  }

  /**
   * Under the {@code validate} profile only: logs the summary of the configuration that just bound
   * and validated (DESIGN §5.6). Creating this bean needs the bound tree, so an invalid one fails
   * startup before anything is logged here.
   */
  @Bean
  @Profile(ValidateProfile.NAME)
  ValidateProfile.Summary kafkafimValidationSummary(
      KafkafimProperties properties, Environment environment) {
    ValidateProfile.Summary summary =
        ValidateProfile.summary(properties, ConfigurationPropertySources.get(environment));
    LOG.info("{}", summary.text());
    return summary;
  }

  /** Static so the guard runs with the other bean factory post-processors, before any singleton. */
  @Bean
  static LocalProfileGuard localProfileGuard() {
    return new LocalProfileGuard();
  }
}
