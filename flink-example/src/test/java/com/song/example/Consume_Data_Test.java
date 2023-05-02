package com.song.example;

import org.junit.Test;

public class Consume_Data_Test {
    
    @Test
	public void LoadTest() {
	
		System.out.println();

		String KAFKA_BOOTSTRAP_SERVER_URL = System.getenv("KAFKA_BOOTSTRAP_SERVER_URL");
		String SCHEMA_REGISTRY_URL = System.getenv("SCHEMA_REGISTRY_URL");
		String TOPIC_NAME = System.getenv("TOPIC_NAME");

        try {

            String[] args = {
				"--bootstrp-server",
				KAFKA_BOOTSTRAP_SERVER_URL,
				"--schema-registry",
				SCHEMA_REGISTRY_URL,
				"--topic",
				TOPIC_NAME
			};


            Consume_Data.main(args);
            
        } catch (Exception e) {

            e.printStackTrace();
            
            String msg = e.getMessage();
            System.out.println(msg);;
        }
	}
}
