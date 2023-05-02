package com.song.example;

import org.junit.Test;

public class Sink_Data_Test {
    
    @Test
	public void SinkTest() {
	
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


            Sink_Data.main(args);

			System.out.println();
			System.out.println("Read the data back");
			Read_Data.main(args);
            
        } catch (Exception e) {

            e.printStackTrace();
            
            String msg = e.getMessage();
            System.out.println(msg);;
        }
	}
}
