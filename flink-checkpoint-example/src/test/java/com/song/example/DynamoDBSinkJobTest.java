package com.song.example;

import org.junit.Test;

public class DynamoDBSinkJobTest {
	@Test
	public void LoadTest() throws Exception {

		var CHECKPOINT_INERVAL = System.getenv("CHECKPOINT_INERVAL");
		var CHECKPOINT_TIMEOUT = System.getenv("CHECKPOINT_TIMEOUT");
		var MAX_CONCURRENT_CHECKPOINTS = System.getenv("MAX_CONCURRENT_CHECKPOINTS");
		var CHECKPOINT_DIR = System.getenv("CHECKPOINT_DIR");

		var BOOTSTRAP_SERVER = System.getenv("BOOTSTRAP_SERVER");
		var SCHEMA_REGISTRY_URL = System.getenv("SCHEMA_REGISTRY_URL");
		var GROUP_ID = System.getenv("GROUP_ID");
		var TOPIC_NAME = System.getenv("TOPIC_NAME");

		String[] args = {
				"--bootstrap-server", BOOTSTRAP_SERVER,
				"--schema-registry", SCHEMA_REGISTRY_URL,
				"--group-id", GROUP_ID,
				"--topic", TOPIC_NAME,
				"--checkpoint-interval", CHECKPOINT_INERVAL,
				"--checkpoint-timeout", CHECKPOINT_TIMEOUT,
				"--max-concurrent-checkpoints", MAX_CONCURRENT_CHECKPOINTS,
				"--checkpoint-dir", CHECKPOINT_DIR
		};

		DynamoDBSinkJob.main(args);

	}
}
