package io.github.matabar.kafkafim;

import java.time.Duration;
import java.util.List;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.DynamicPropertyRegistrar;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.grafana.LgtmStackContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Throwaway Kafka, Elasticsearch and Grafana LGTM containers for integration tests and local runs.
 * Kafka and Elasticsearch addresses are exposed through kafkafim's own cluster properties ({@code
 * kafkafim.kafka.clusters.local}, {@code kafkafim.elasticsearch.clusters.local}), since kafkafim
 * manages its clients itself rather than through Spring Boot's single-cluster auto-configuration.
 */
@TestConfiguration(proxyBeanMethods = false)
class TestcontainersConfiguration {

  @Bean
  @ServiceConnection
  LgtmStackContainer grafanaLgtmContainer() {
    return new LgtmStackContainer(DockerImageName.parse("grafana/otel-lgtm:0.11.0"));
  }

  @Bean
  KafkaContainer kafkaContainer() {
    return KafkaTestContainers.kafka();
  }

  @Bean
  ElasticsearchContainer elasticsearchContainer() {
    return new ElasticsearchContainer(
            DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.19.0"))
        .withEnv("xpack.security.enabled", "false")
        .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
        // Parallel Failsafe forks can boot several nodes at once; the 60 s default is tight.
        .withStartupTimeout(Duration.ofMinutes(3));
  }

  @Bean
  DynamicPropertyRegistrar kafkafimClusterProperties(
      KafkaContainer kafka, ElasticsearchContainer elasticsearch) {
    return registry -> {
      registry.add(
          "kafkafim.kafka.clusters.local.bootstrap-servers",
          () -> List.of(kafka.getBootstrapServers()));
      registry.add(
          "kafkafim.elasticsearch.clusters.local.uris",
          () -> List.of("http://" + elasticsearch.getHttpHostAddress()));
    };
  }
}
