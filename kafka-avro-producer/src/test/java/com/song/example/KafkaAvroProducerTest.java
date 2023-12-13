package com.song.example;

import org.junit.Test;

public class KafkaAvroProducerTest {

	@Test
	public void Test() throws Exception {

		var BOOTSTRAP_SERVERS = System.getenv("BOOTSTRAP_SERVERS");
		var SCHEMA_REGISTRY = System.getenv("SCHEMA_REGISTRY");

		var TOPIC = System.getenv("TOPIC");
		var SCHEMA_FILE = System.getenv("SCHEMA_FILE");
		var DATA_FILE = System.getenv("DATA_FILE");

		String[] args = {
				"--bootstrap-server", BOOTSTRAP_SERVERS,
				"--schema-registry", SCHEMA_REGISTRY,
				"--topic", TOPIC,
				"-schema-file", SCHEMA_FILE,
				"--data-file", DATA_FILE
		};

		KafkaAvroProducer.main(args);
		System.out.println();
	}
}
