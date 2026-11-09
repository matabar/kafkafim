package io.github.matabar.kafkafim;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;

@Import(TestcontainersConfiguration.class)
// Start the LGTM, Kafka and Elasticsearch beans at the same time instead of one after another.
// No container reads another's address: the DynamicPropertyRegistrar's suppliers and LGTM's
// @ServiceConnection are only read once every container is running.
@SpringBootTest(properties = "spring.testcontainers.beans.startup=parallel")
// Close the context (and stop its containers) after the class instead of caching it until the
// fork exits; with reuseForks the fork goes on to run other container-heavy classes.
@DirtiesContext
class KafkafimApplicationIntegrationTest {

  @Test
  void contextLoads() {}
}
