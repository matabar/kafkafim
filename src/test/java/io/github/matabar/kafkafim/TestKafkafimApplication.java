package io.github.matabar.kafkafim;

import org.springframework.boot.SpringApplication;

public class TestKafkafimApplication {

	public static void main(String[] args) {
		SpringApplication.from(KafkafimApplication::main).with(TestcontainersConfiguration.class).run(args);
	}

}
