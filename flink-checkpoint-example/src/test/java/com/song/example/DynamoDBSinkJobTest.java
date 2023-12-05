package com.song.example;

import org.junit.Test;

public class DynamoDBSinkJobTest {
	@Test
	public void LoadTest() throws Exception {

		var SCHEMA_REGISTRY_URL = System.getenv("SCHEMA_REGISTRY_URL");
		var TOPIC_NAME = System.getenv("TOPIC_NAME");

		String[] args = {
			"--schema-registry", SCHEMA_REGISTRY_URL,
			"--topic", TOPIC_NAME
		};
		DynamoDBSinkJob.main(args);

	}
}
