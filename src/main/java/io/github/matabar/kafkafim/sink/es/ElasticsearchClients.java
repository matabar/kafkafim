package io.github.matabar.kafkafim.sink.es;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.ElasticsearchIdentity;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Keystore;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Trust;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.util.Timeout;
import org.jspecify.annotations.Nullable;

/**
 * Builds one low-level client per {@code (cluster, identity)} from the bound configuration (DESIGN
 * §5.1, §5.2.1): node URIs with the client's round-robin node selection and dead-node retry, the
 * cluster CA as the only trust anchor when {@code ssl.ca-certificate} is set (otherwise the JVM's
 * default trust), and the identity as a preemptive {@code Authorization: Basic} or {@code
 * Authorization: ApiKey} default header and/or a client-certificate keystore. No identity means no
 * {@code Authorization} header (plain {@code http}, local development).
 *
 * <p>Secrets never appear in exceptions or logs: messages name the cluster, the identity and file
 * paths only, and the client's own request logging does not print headers.
 */
public final class ElasticsearchClients {

  private ElasticsearchClients() {}

  /**
   * Builds the client for one {@code (cluster, identity)}.
   *
   * @param clusterName cluster name, for messages
   * @param cluster the validated cluster configuration
   * @param identityName identity name, or {@code null} for no credentials
   * @param settings timeouts and pool sizes
   * @return a started client; the caller closes it
   * @throws IllegalArgumentException if the identity does not exist on the cluster
   * @throws IllegalStateException if the CA certificate or keystore cannot be loaded
   */
  public static Rest5Client create(
      String clusterName,
      ElasticsearchCluster cluster,
      @Nullable String identityName,
      ClientSettings settings) {
    ElasticsearchIdentity identity = null;
    if (identityName != null) {
      identity = cluster.identities().get(identityName);
      if (identity == null) {
        throw new IllegalArgumentException(
            "Elasticsearch cluster '" + clusterName + "' has no identity '" + identityName + "'");
      }
    }
    Rest5ClientBuilder builder =
        Rest5Client.builder(cluster.parsedUris())
            .setCompressionEnabled(cluster.compression())
            .setDefaultHeaders(authorization(identity))
            .setConnectionConfigCallback(
                config -> config.setConnectTimeout(Timeout.of(settings.connectTimeout())))
            .setRequestConfigCallback(config ->
                config
                    .setResponseTimeout(Timeout.of(settings.responseTimeout()))
                    .setConnectionRequestTimeout(Timeout.of(settings.connectionRequestTimeout())))
            .setConnectionManagerCallback(pool ->
                pool.setMaxConnPerRoute(settings.maxConnectionsPerNode())
                    .setMaxConnTotal(settings.maxConnectionsTotal()));
    if (cluster.usesHttps()) {
      SSLContext sslContext =
          sslContext(
              clusterName, identityName, cluster.ssl(), identity == null ? null : identity.ssl());
      if (sslContext != null) {
        builder.setSSLContext(sslContext);
      }
    }
    return builder.build();
  }

  /** The default {@code Authorization} header for an identity, or none. */
  static Header[] authorization(@Nullable ElasticsearchIdentity identity) {
    if (identity == null) {
      return new Header[0];
    }
    if (identity.username() != null) {
      String password = identity.password() == null ? "" : identity.password();
      String token =
          Base64.getEncoder()
              .encodeToString(
                  (identity.username() + ":" + password).getBytes(StandardCharsets.UTF_8));
      return new Header[] {new BasicHeader(HttpHeaders.AUTHORIZATION, "Basic " + token, true)};
    }
    if (identity.apiKey() != null) {
      return new Header[] {
        new BasicHeader(HttpHeaders.AUTHORIZATION, "ApiKey " + identity.apiKey(), true)
      };
    }
    return new Header[0];
  }

  /**
   * The TLS context for a cluster and identity, or {@code null} when neither a CA nor a keystore is
   * configured (the client then uses the JVM default).
   */
  static @Nullable SSLContext sslContext(
      String clusterName,
      @Nullable String identityName,
      @Nullable Trust trust,
      @Nullable Keystore keystore) {
    if (trust == null && keystore == null) {
      return null;
    }
    TrustManager[] trustManagers =
        trust == null ? null : trustManagers(clusterName, Path.of(trust.caCertificate()));
    KeyManager[] keyManagers =
        keystore == null ? null : keyManagers(clusterName, identityName, keystore);
    try {
      SSLContext context = SSLContext.getInstance("TLS");
      context.init(keyManagers, trustManagers, null);
      return context;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException(
          "Cannot initialise TLS for Elasticsearch cluster '" + clusterName + "'", e);
    }
  }

  private static TrustManager[] trustManagers(String clusterName, Path caCertificate) {
    try (InputStream in = Files.newInputStream(caCertificate)) {
      Collection<? extends Certificate> certificates =
          CertificateFactory.getInstance("X.509").generateCertificates(in);
      if (certificates.isEmpty()) {
        throw new IllegalStateException(
            "No certificate in the CA file of Elasticsearch cluster '"
                + clusterName
                + "': "
                + caCertificate);
      }
      KeyStore store = KeyStore.getInstance(KeyStore.getDefaultType());
      store.load(null, null);
      List<Certificate> list = new ArrayList<>(certificates);
      for (int i = 0; i < list.size(); i++) {
        store.setCertificateEntry("ca-" + i, list.get(i));
      }
      TrustManagerFactory factory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      factory.init(store);
      return factory.getTrustManagers();
    } catch (IOException | GeneralSecurityException e) {
      throw new IllegalStateException(
          "Cannot load the CA certificate of Elasticsearch cluster '"
              + clusterName
              + "' from "
              + caCertificate
              + " ("
              + e.getClass().getSimpleName()
              + ")",
          e);
    }
  }

  private static KeyManager[] keyManagers(
      String clusterName, @Nullable String identityName, Keystore keystore) {
    Path location = Path.of(keystore.keystoreLocation());
    char[] password =
        keystore.keystorePassword() == null
            ? new char[0]
            : keystore.keystorePassword().toCharArray();
    String type = keystore.keystoreType() == null ? "PKCS12" : keystore.keystoreType();
    try (InputStream in = Files.newInputStream(location)) {
      KeyStore store = KeyStore.getInstance(type);
      store.load(in, password);
      KeyManagerFactory factory =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      factory.init(store, password);
      return factory.getKeyManagers();
    } catch (IOException | GeneralSecurityException e) {
      // The cause's message never contains the password (JDK keystore messages do not echo it).
      throw new IllegalStateException(
          "Cannot load the client keystore of identity '"
              + identityName
              + "' on Elasticsearch cluster '"
              + clusterName
              + "' from "
              + location
              + " ("
              + e.getClass().getSimpleName()
              + ")",
          e);
    }
  }
}
