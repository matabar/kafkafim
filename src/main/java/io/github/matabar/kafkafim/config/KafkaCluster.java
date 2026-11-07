package io.github.matabar.kafkafim.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A named Kafka connection, {@code kafkafim.kafka.clusters.<name>} (DESIGN §5.1). Trust material
 * and the security protocol belong to the cluster and are shared by all of its identities.
 *
 * @param bootstrapServers {@code host:port} entries
 * @param securityProtocol transport and authentication protocol; default {@code PLAINTEXT}
 * @param ssl cluster-wide trust (truststore), shared by all identities; optional
 * @param identities named credential sets referenced by pipelines (§5.2.1)
 * @param defaultIdentity identity used when a pipeline names none; optional
 * @param properties passthrough to kafka-clients, applied before kafkafim's own settings
 */
public record KafkaCluster(
    @NotEmpty List<String> bootstrapServers,
    SecurityProtocol securityProtocol,
    @Valid Trust ssl,
    Map<String, @Valid KafkaIdentity> identities,
    String defaultIdentity,
    Map<String, String> properties) {

  /** Applies the documented defaults. */
  public KafkaCluster {
    bootstrapServers = Defaults.list(bootstrapServers);
    securityProtocol = Defaults.orDefault(securityProtocol, SecurityProtocol.PLAINTEXT);
    identities = Defaults.map(identities);
    properties = Defaults.map(properties);
  }

  /** Kafka {@code security.protocol} values. */
  public enum SecurityProtocol {
    /** No TLS, no authentication. */
    PLAINTEXT,
    /** TLS; client certificates optional (decided by the broker). */
    SSL,
    /** SASL authentication without TLS. */
    SASL_PLAINTEXT,
    /** SASL authentication over TLS. */
    SASL_SSL;

    /** Whether the protocol requires a SASL identity on every connection. */
    public boolean requiresSasl() {
      return this == SASL_PLAINTEXT || this == SASL_SSL;
    }

    /** Whether the protocol runs over TLS, so trust and keystores apply. */
    public boolean usesTls() {
      return this == SSL || this == SASL_SSL;
    }
  }

  /**
   * Cluster-wide trust material.
   *
   * @param truststoreLocation path to the truststore file
   * @param truststorePassword truststore password; must come from a placeholder (§5.5)
   * @param truststoreType {@code PKCS12} (default when unset, decided by kafka-clients) or {@code
   *     JKS}/{@code PEM}
   */
  public record Trust(
      @NotBlank String truststoreLocation, String truststorePassword, String truststoreType) {}

  /**
   * A named Kafka credential set: SASL, a client-certificate keystore, or both (§5.2.1).
   *
   * @param sasl SASL credentials; optional
   * @param ssl client-certificate keystore for mTLS; optional
   */
  public record KafkaIdentity(@Valid Sasl sasl, @Valid Keystore ssl) {}

  /**
   * SASL credentials. GSSAPI and OAUTHBEARER take their extra settings from the cluster's {@code
   * properties}; PLAIN and SCRAM need a username and password.
   *
   * @param mechanism SASL mechanism, e.g. {@code SCRAM-SHA-256}
   * @param username user name
   * @param password password; must come from a placeholder (§5.5)
   */
  public record Sasl(@NotBlank String mechanism, String username, String password) {

    private static final Set<String> PASSWORD_MECHANISMS =
        Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");

    /** Whether the mechanism authenticates with a username and password. */
    public boolean needsUsernameAndPassword() {
      return mechanism != null && PASSWORD_MECHANISMS.contains(mechanism);
    }
  }

  /**
   * A client-certificate keystore.
   *
   * @param keystoreLocation path to the keystore file
   * @param keystorePassword keystore password; must come from a placeholder (§5.5)
   * @param keyPassword private key password if it differs; optional
   * @param keystoreType {@code PKCS12} (default when unset) or {@code JKS}/{@code PEM}
   */
  public record Keystore(
      @NotBlank String keystoreLocation,
      String keystorePassword,
      String keyPassword,
      String keystoreType) {}
}
