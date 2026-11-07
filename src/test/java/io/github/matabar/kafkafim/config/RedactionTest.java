package io.github.matabar.kafkafim.config;

import static io.github.matabar.kafkafim.config.ConfigTestSupport.MINIMAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.RecordComponent;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** No secret value appears in the {@code toString()} of the configuration model (DESIGN §5.5). */
class RedactionTest {

  private static final String K = "kafkafim.kafka.clusters.k";
  private static final String ES = "kafkafim.elasticsearch.clusters.es";

  /** Every secret the tree below sets; each value is unique so a leak names its field. */
  private static final List<String> SECRETS =
      List.of(
          "S3CRET-truststore",
          "S3CRET-sasl",
          "S3CRET-keystore",
          "S3CRET-key",
          "S3CRET-jaas",
          "S3CRET-passthrough-key-password",
          "S3CRET-basic-auth-info",
          "S3CRET-es-password",
          "S3CRET-es-keystore",
          "S3CRET-api-key");

  private static KafkafimProperties treeWithEverySecret() {
    return ConfigTestSupport.bind(
        MINIMAL,
        "kafkafim.allow-inline-secrets=true",
        K + ".security-protocol=SASL_SSL",
        K + ".ssl.truststore-location=/trust.p12",
        K + ".ssl.truststore-password=S3CRET-truststore",
        K + ".identities.r.sasl.mechanism=SCRAM-SHA-256",
        K + ".identities.r.sasl.username=reader-user",
        K + ".identities.r.sasl.password=S3CRET-sasl",
        K + ".identities.r.ssl.keystore-location=/client.p12",
        K + ".identities.r.ssl.keystore-password=S3CRET-keystore",
        K + ".identities.r.ssl.key-password=S3CRET-key",
        K + ".default-identity=r",
        K + ".properties[sasl.jaas.config]=S3CRET-jaas",
        K + ".properties[ssl.key.password]=S3CRET-passthrough-key-password",
        K + ".properties[basic.auth.user.info]=S3CRET-basic-auth-info",
        K + ".properties[client.id]=visible-client-id",
        ES + ".uris[0]=https://es:9200",
        ES + ".identities.basic.username=writer-user",
        ES + ".identities.basic.password=S3CRET-es-password",
        ES + ".identities.basic.ssl.keystore-location=/es-client.p12",
        ES + ".identities.basic.ssl.keystore-password=S3CRET-es-keystore",
        ES + ".identities.key.api-key=S3CRET-api-key",
        ES + ".default-identity=basic");
  }

  @Test
  void theWholeTreePrintsNoSecret() {
    String printed = treeWithEverySecret().toString();

    assertThat(printed).doesNotContain("S3CRET");
    assertThat(printed)
        .contains("reader-user", "writer-user", "visible-client-id", "/client.p12")
        .contains("sasl.jaas.config=" + Redaction.MASK);
  }

  @Test
  void theTreeReallyHoldsTheSecrets() {
    // Guards the test above: the secrets are bound, only their printing is masked.
    KafkafimProperties properties = treeWithEverySecret();
    KafkaCluster kafka = properties.kafka().clusters().get("k");
    ElasticsearchCluster es = properties.elasticsearch().clusters().get("es");

    assertThat(
            List.of(
                kafka.ssl().truststorePassword(),
                kafka.identities().get("r").sasl().password(),
                kafka.identities().get("r").ssl().keystorePassword(),
                kafka.identities().get("r").ssl().keyPassword(),
                kafka.properties().get("sasl.jaas.config"),
                kafka.properties().get("ssl.key.password"),
                kafka.properties().get("basic.auth.user.info"),
                es.identities().get("basic").password(),
                es.identities().get("basic").ssl().keystorePassword(),
                es.identities().get("key").apiKey()))
        .containsExactlyElementsOf(SECRETS);
  }

  @Test
  void eachSecretHoldingRecordMasksItsSecrets() {
    assertThat(new KafkaCluster.Trust("/t", "S3CRET", "PKCS12").toString())
        .isEqualTo(
            "Trust[truststoreLocation=/t, truststorePassword=******, truststoreType=PKCS12]");
    assertThat(new KafkaCluster.Sasl("PLAIN", "user", "S3CRET").toString())
        .isEqualTo("Sasl[mechanism=PLAIN, username=user, password=******]");
    assertThat(new KafkaCluster.Keystore("/k", "S3CRET", "S3CRET", null).toString())
        .isEqualTo(
            "Keystore[keystoreLocation=/k, keystorePassword=******, keyPassword=******,"
                + " keystoreType=null]");
    assertThat(
            new KafkaCluster(
                    List.of("k:9092"),
                    null,
                    null,
                    null,
                    null,
                    Map.of("sasl.jaas.config", "S3CRET", "client.id", "c"))
                .toString())
        .doesNotContain("S3CRET")
        .contains("sasl.jaas.config=******", "client.id=c");
    assertThat(
            new ElasticsearchCluster.ElasticsearchIdentity("user", "S3CRET", null, null).toString())
        .isEqualTo("ElasticsearchIdentity[username=user, password=******, apiKey=null, ssl=null]");
    assertThat(
            new ElasticsearchCluster.ElasticsearchIdentity(null, null, "S3CRET", null).toString())
        .isEqualTo("ElasticsearchIdentity[username=null, password=null, apiKey=******, ssl=null]");
    assertThat(new ElasticsearchCluster.Keystore("/k", "S3CRET", "JKS").toString())
        .isEqualTo("Keystore[keystoreLocation=/k, keystorePassword=******, keystoreType=JKS]");
  }

  @Test
  void everySecretHoldingRecordOverridesToString() {
    // A secret component added later without a toString override would print in clear.
    List<String> missing = new ArrayList<>();
    for (Class<?> record : recordsReachableFrom(KafkafimProperties.class)) {
      boolean holdsSecret = false;
      for (RecordComponent component : record.getRecordComponents()) {
        String name = component.getName().toLowerCase(Locale.ROOT);
        holdsSecret |=
            name.contains("password")
                || name.contains("secret")
                || name.contains("apikey")
                || name.equals("properties");
      }
      if (holdsSecret && !declaresToString(record)) {
        missing.add(record.getName());
      }
    }
    assertThat(missing).isEmpty();
  }

  private static boolean declaresToString(Class<?> type) {
    try {
      type.getDeclaredMethod("toString");
      return true;
    } catch (NoSuchMethodException e) {
      return false;
    }
  }

  private static Set<Class<?>> recordsReachableFrom(Class<?> root) {
    Set<Class<?>> seen = new HashSet<>();
    List<Type> pending = new ArrayList<>(List.of(root));
    while (!pending.isEmpty()) {
      Type type = pending.removeLast();
      if (type instanceof ParameterizedType parameterized) {
        pending.addAll(List.of(parameterized.getActualTypeArguments()));
      } else if (type instanceof Class<?> cls && cls.isRecord() && seen.add(cls)) {
        for (RecordComponent component : cls.getRecordComponents()) {
          pending.add(component.getGenericType());
        }
      }
    }
    assertThat(seen).contains(KafkaCluster.Sasl.class, ElasticsearchCluster.Keystore.class);
    return seen;
  }
}
