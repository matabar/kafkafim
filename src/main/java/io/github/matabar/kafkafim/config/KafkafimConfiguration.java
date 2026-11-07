package io.github.matabar.kafkafim.config;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Role;
import org.springframework.core.env.Environment;

/** Binds and validates {@code kafkafim.*}. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkafimProperties.class)
public class KafkafimConfiguration {

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
    return new KafkafimPropertiesValidator(() -> ConfigurationPropertySources.get(environment));
  }
}
