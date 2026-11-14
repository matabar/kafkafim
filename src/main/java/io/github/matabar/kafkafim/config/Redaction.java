package io.github.matabar.kafkafim.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Masks secret values in the {@code toString()} of configuration records, so a logged or printed
 * configuration never shows a password, API key or keystore password (DESIGN §5.5). Usernames,
 * paths and other settings stay readable.
 *
 * <p>Its sibling {@link KafkaClientSecurity#redacted} masks the kafka-clients settings that are
 * logged when a client is created. The two differ on purpose: this one prints {@link #MASK} and
 * masks only the keys the inline-secret check treats as secrets, since it prints configuration that
 * has not been through the client; that one prints {@code [hidden]} and uses a broader key rule,
 * since the built settings can carry values the configuration never named.
 */
final class Redaction {

  /** Printed in place of a secret that is set. */
  static final String MASK = "******";

  private Redaction() {}

  /** {@link #MASK} for a set secret; {@code null} stays {@code null}, so "unset" is visible. */
  static String mask(Object secret) {
    return secret == null ? "null" : MASK;
  }

  /**
   * A copy of a kafka-clients passthrough map with the values of secret keys masked, by the same
   * key rules as the inline-secret check ({@link InlineSecretCheck#isSecretPassthroughKey}).
   */
  static Map<String, String> passthrough(Map<String, String> properties) {
    Map<String, String> masked = new LinkedHashMap<>();
    properties.forEach((key, value) ->
        masked.put(key, InlineSecretCheck.isSecretPassthroughKey(key) ? mask(value) : value));
    return masked;
  }
}
