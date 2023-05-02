package com.song.example;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema.Field;
import org.junit.Test;

import com.song.example.utilities.SchemaRegistry;

public class SchemaRegistryTest {

	@Test
	public void LoadSchemaTest() throws IOException {
		
		String SCHEMA_REGISTRY_URL = System.getenv("SCHEMA_REGISTRY_URL");
		String TOPIC_NAME = System.getenv("TOPIC_NAME");

		var sr = new SchemaRegistry(SCHEMA_REGISTRY_URL, TOPIC_NAME + "-value");
		var schema = sr.LoadSchema();

		{
            System.out.println();

            List<Field> fields = schema.getFields();
            for (Field f : fields) {
                System.out.println(f.name() + " - " + f.schema().getType());
            }

			System.out.println();
        }

	}
}