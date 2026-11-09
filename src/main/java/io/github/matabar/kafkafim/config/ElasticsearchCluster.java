package io.github.matabar.kafkafim.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.springframework.util.unit.DataSize;

/**
 * A named Elasticsearch connection, {@code kafkafim.elasticsearch.clusters.<name>} (DESIGN §5.1).
 * Trust (CA) belongs to the cluster and is shared by all of its identities.
 *
 * @param uris {@code http://} or {@code https://} node URIs, all with the same scheme
 * @param ssl cluster-wide trust; optional
 * @param identities named credential sets referenced by routes (§5.2.1)
 * @param defaultIdentity identity used when a route names none; optional
 * @param bulk bulk batching limits per {@code (cluster, identity)}
 * @param compression gzip request bodies; default {@code true}
 * @param client HTTP client timeouts of every client of this cluster
 */
public record ElasticsearchCluster(
    @NotEmpty List<String> uris,
    @Valid Trust ssl,
    Map<String, @Valid ElasticsearchIdentity> identities,
    String defaultIdentity,
    @Valid Bulk bulk,
    Boolean compression,
    @Valid Client client) {

  /** Applies the documented defaults. */
  public ElasticsearchCluster {
    uris = Defaults.list(uris);
    identities = Defaults.map(identities);
    bulk = Defaults.orDefault(bulk, new Bulk(null, null, null, null));
    compression = Defaults.orDefault(compression, Boolean.TRUE);
    client = Defaults.orDefault(client, new Client(null, null));
  }

  /**
   * Whether every URI uses {@code https}. Only meaningful once validation has checked that all URIs
   * share one scheme.
   */
  public boolean usesHttps() {
    return !uris.isEmpty()
        && uris.stream().allMatch(uri -> uri != null && uri.startsWith("https://"));
  }

  /** The URIs parsed; call only on a validated model. */
  public List<URI> parsedUris() {
    return uris.stream().map(URI::create).toList();
  }

  /**
   * Cluster-wide trust.
   *
   * @param caCertificate path to the PEM CA certificate that signs the cluster's HTTP certificate
   */
  public record Trust(@NotBlank String caCertificate) {}

  /**
   * A named Elasticsearch credential set: exactly one of {@code username}+{@code password}, {@code
   * api-key}, or an {@code ssl} keystore; a keystore may be combined with basic auth or an API key
   * (§5.2.1).
   *
   * @param username basic-auth user
   * @param password basic-auth password; must come from a placeholder (§5.5)
   * @param apiKey encoded API key; must come from a placeholder (§5.5)
   * @param ssl client-certificate keystore (PKI realm); optional
   */
  public record ElasticsearchIdentity(
      String username, String password, String apiKey, @Valid Keystore ssl) {

    /** Whether basic-auth fields are present (complete or not). */
    public boolean hasBasic() {
      return username != null || password != null;
    }

    /** Masks the password and API key; the username stays (§5.5). */
    @Override
    public String toString() {
      return "ElasticsearchIdentity[username="
          + username
          + ", password="
          + Redaction.mask(password)
          + ", apiKey="
          + Redaction.mask(apiKey)
          + ", ssl="
          + ssl
          + "]";
    }
  }

  /**
   * A client-certificate keystore.
   *
   * @param keystoreLocation path to the keystore file
   * @param keystorePassword keystore password; must come from a placeholder (§5.5)
   * @param keystoreType {@code PKCS12} (default when unset) or {@code JKS}
   */
  public record Keystore(
      @NotBlank String keystoreLocation, String keystorePassword, String keystoreType) {

    /** Masks the password (§5.5). */
    @Override
    public String toString() {
      return "Keystore[keystoreLocation="
          + keystoreLocation
          + ", keystorePassword="
          + Redaction.mask(keystorePassword)
          + ", keystoreType="
          + keystoreType
          + "]";
    }
  }

  /**
   * Bulk batching limits; a batch flushes on whichever is reached first.
   *
   * @param maxDocs documents per bulk request; default 5000
   * @param maxBytes body size per bulk request; default 5MB
   * @param maxAge age of the oldest document in a batch; default 200ms
   * @param inFlight concurrent bulk requests per {@code (cluster, identity)}; default 4
   */
  public record Bulk(
      @Positive Integer maxDocs, DataSize maxBytes, Duration maxAge, @Positive Integer inFlight) {

    /** Applies the documented defaults. */
    public Bulk {
      maxDocs = Defaults.orDefault(maxDocs, 5000);
      maxBytes = Defaults.orDefault(maxBytes, DataSize.ofMegabytes(5));
      maxAge = Defaults.orDefault(maxAge, Duration.ofMillis(200));
      inFlight = Defaults.orDefault(inFlight, 4);
    }
  }

  /**
   * HTTP client timeouts, shared by every identity's client of the cluster (DESIGN §7, ADR-0031).
   * The connection pool stays a code default.
   *
   * @param connectTimeout TCP and TLS connect timeout per node; default 5s
   * @param responseTimeout socket inactivity timeout while waiting for a response; a bulk request
   *     of {@code bulk.max-bytes} must be answered within it, and a timeout is a request-level
   *     retry; default 60s
   */
  public record Client(Duration connectTimeout, Duration responseTimeout) {

    /** Applies the documented defaults. */
    public Client {
      connectTimeout = Defaults.orDefault(connectTimeout, Duration.ofSeconds(5));
      responseTimeout = Defaults.orDefault(responseTimeout, Duration.ofSeconds(60));
    }
  }
}
