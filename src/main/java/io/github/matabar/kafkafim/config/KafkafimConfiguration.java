package io.github.matabar.kafkafim.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Binds and validates {@code kafkafim.*}. */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(KafkafimProperties.class)
public class KafkafimConfiguration {}
