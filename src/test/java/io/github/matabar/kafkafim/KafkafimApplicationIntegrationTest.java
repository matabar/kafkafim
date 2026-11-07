package io.github.matabar.kafkafim;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class KafkafimApplicationIntegrationTest {

	@Test
	void contextLoads() {
	}

}
