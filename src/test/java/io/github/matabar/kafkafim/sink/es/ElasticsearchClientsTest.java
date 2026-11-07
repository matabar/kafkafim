package io.github.matabar.kafkafim.sink.es;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import io.github.matabar.kafkafim.config.ElasticsearchCluster;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.ElasticsearchIdentity;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Keystore;
import io.github.matabar.kafkafim.config.ElasticsearchCluster.Trust;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.http.Header;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ElasticsearchClientsTest {

  static final String KEYSTORE_PASSWORD = "s3cret-keystore-pw";

  @TempDir static Path dir;
  static Path keystore;
  static Path caPem;

  @BeforeAll
  static void createKeyMaterial() throws Exception {
    keystore = dir.resolve("client.p12");
    caPem = dir.resolve("ca.crt");
    keytool(
        "-genkeypair",
        "-alias",
        "client",
        "-keyalg",
        "EC",
        "-dname",
        "CN=kafkafim-test",
        "-validity",
        "2",
        "-storetype",
        "PKCS12",
        "-keystore",
        keystore.toString(),
        "-storepass",
        KEYSTORE_PASSWORD);
    keytool(
        "-exportcert",
        "-rfc",
        "-alias",
        "client",
        "-keystore",
        keystore.toString(),
        "-storepass",
        KEYSTORE_PASSWORD,
        "-file",
        caPem.toString());
  }

  static void keytool(String... args) throws Exception {
    Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
    List<String> command = new java.util.ArrayList<>(List.of(keytool.toString()));
    command.addAll(List.of(args));
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(process.waitFor()).as(output).isZero();
  }

  static ElasticsearchCluster cluster(
      String uri, Trust trust, Map<String, ElasticsearchIdentity> identities) {
    return new ElasticsearchCluster(List.of(uri), trust, identities, null, null, null);
  }

  @Test
  void basicAuthHeader() {
    Header[] headers =
        ElasticsearchClients.authorization(new ElasticsearchIdentity("user", "pä:ss", null, null));
    assertThat(headers)
        .singleElement()
        .satisfies(h -> {
          assertThat(h.getName()).isEqualTo("Authorization");
          assertThat(h.isSensitive()).isTrue();
          assertThat(h.getValue())
              .isEqualTo(
                  "Basic "
                      + Base64.getEncoder()
                          .encodeToString("user:pä:ss".getBytes(StandardCharsets.UTF_8)));
        });
  }

  @Test
  void apiKeyHeader() {
    assertThat(
            ElasticsearchClients.authorization(
                new ElasticsearchIdentity(null, null, "ZW5jb2RlZA==", null)))
        .singleElement()
        .satisfies(h -> assertThat(h.getValue()).isEqualTo("ApiKey ZW5jb2RlZA=="));
  }

  @Test
  void noIdentityOrKeystoreOnlyMeansNoHeader() {
    assertThat(ElasticsearchClients.authorization(null)).isEmpty();
    assertThat(
            ElasticsearchClients.authorization(
                new ElasticsearchIdentity(
                    null, null, null, new Keystore(keystore.toString(), KEYSTORE_PASSWORD, null))))
        .isEmpty();
  }

  @Test
  void tlsFromCaAndClientKeystore() {
    SSLContext context =
        ElasticsearchClients.sslContext(
            "search-eu",
            "platform-cert",
            new Trust(caPem.toString()),
            new Keystore(keystore.toString(), KEYSTORE_PASSWORD, "PKCS12"));
    assertThat(context).isNotNull();
    assertThat(context.getProtocol()).isEqualTo("TLS");
  }

  @Test
  void noTrustAndNoKeystoreUsesTheJvmDefault() {
    assertThat(ElasticsearchClients.sslContext("search-eu", null, null, null)).isNull();
  }

  @Test
  void wrongKeystorePasswordNamesIdentityButNotThePassword() {
    assertThatIllegalStateException()
        .isThrownBy(() ->
            ElasticsearchClients.sslContext(
                "search-eu",
                "platform-cert",
                null,
                new Keystore(keystore.toString(), "wrong-password-value", null)))
        .withMessageContaining("platform-cert")
        .withMessageContaining("search-eu")
        .withMessageNotContaining("wrong-password-value")
        .satisfies(e -> {
          for (Throwable t = e; t != null; t = t.getCause()) {
            assertThat(String.valueOf(t.getMessage())).doesNotContain("wrong-password-value");
          }
        });
  }

  @Test
  void unreadableCaNamesTheClusterAndPath() throws Exception {
    Path notPem = Files.writeString(dir.resolve("not.pem"), "not a certificate");
    assertThatIllegalStateException()
        .isThrownBy(() ->
            ElasticsearchClients.sslContext("search-eu", null, new Trust(notPem.toString()), null))
        .withMessageContaining("search-eu")
        .withMessageContaining(notPem.toString());
  }

  @Test
  void createsClientsForHttpWithoutIdentityAndHttpsWithCa() throws Exception {
    try (Rest5Client plain =
        ElasticsearchClients.create(
            "local",
            cluster("http://localhost:9200", null, Map.of()),
            null,
            ClientSettings.DEFAULT)) {
      assertThat(plain.getNodes()).hasSize(1);
    }
    ElasticsearchCluster secure =
        new ElasticsearchCluster(
            List.of("https://es-1:9200", "https://es-2:9200"),
            new Trust(caPem.toString()),
            Map.of("writer", new ElasticsearchIdentity("u", "p", null, null)),
            "writer",
            null,
            false);
    try (Rest5Client client =
        ElasticsearchClients.create("search-eu", secure, "writer", ClientSettings.DEFAULT)) {
      assertThat(client.getNodes()).hasSize(2);
    }
  }

  @Test
  void unknownIdentityIsRejected() {
    assertThatIllegalArgumentException()
        .isThrownBy(() ->
            ElasticsearchClients.create(
                "local",
                cluster("http://localhost:9200", null, Map.of()),
                "nobody",
                ClientSettings.DEFAULT))
        .withMessageContaining("nobody");
  }
}
