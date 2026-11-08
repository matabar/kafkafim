package io.github.matabar.kafkafim.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.matabar.kafkafim.KafkafimApplication;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;

/**
 * Starts the real application with an invalid tree that holds literal secrets and checks the
 * printed failure analysis (DESIGN §5.5, §5.6): it lists the violations, including a Bean
 * Validation field error next to a secret, but never a secret value. Fails at binding, before any
 * client is created, so no Docker is needed.
 */
@ExtendWith(OutputCaptureExtension.class)
class SecretRedactionIntegrationTest {

  private static final String K = "kafkafim.kafka.clusters.k";
  private static final String ES = "kafkafim.elasticsearch.clusters.es";

  @Test
  void theFailureAnalysisNamesViolationsButNoSecretValue(CapturedOutput output) {
    SpringApplicationBuilder application =
        new SpringApplicationBuilder(KafkafimApplication.class)
            .profiles("kafkafim")
            .properties(
                "spring.main.cloud-platform=none",
                "spring.docker.compose.enabled=false",
                K + ".bootstrap-servers[0]=k1:9092",
                K + ".security-protocol=SASL_SSL",
                K + ".ssl.truststore-location=/trust.p12",
                K + ".ssl.truststore-password=S3CRET-truststore",
                K + ".identities.r.sasl.mechanism=SCRAM-SHA-256",
                K + ".identities.r.sasl.username=reader-user",
                K + ".identities.r.sasl.password=S3CRET-sasl",
                K + ".properties[sasl.jaas.config]=S3CRET-jaas",
                ES + ".uris[0]=https://es:9200",
                ES + ".identities.w.api-key=S3CRET-api-key",
                // Bean Validation field error (blank location) on the record holding a secret.
                ES + ".identities.w.ssl.keystore-location=",
                ES + ".identities.w.ssl.keystore-password=S3CRET-keystore");

    // A command-line argument beats application.yaml's 8080; a default property does not.
    assertThatThrownBy(() -> application.run("--server.port=0")).isNotNull();

    assertThat(output.toString())
        .contains("APPLICATION FAILED TO START")
        .contains(K + ".identities.r.sasl.password: literal secret value")
        .contains("keystoreLocation")
        .doesNotContain("S3CRET");
  }
}
