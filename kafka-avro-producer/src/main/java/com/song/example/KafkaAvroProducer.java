package com.song.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

public class KafkaAvroProducer {

	private static String KEY_SCHEMA_STRING = "{\"type\": \"record\",\"name\": \"key\",\"fields\":[{\"type\": \"string\",\"name\": \"key\"}]}}";

	public static void main(String[] args) throws Exception {
		var parameter = ParameterTool.fromArgs(args);

		var BOOTSTRAP_SERVERS = parameter.get("bootstrap-server");
		var SCHEMA_REGISTRY = parameter.get("schema-registry");

		var TOPIC = parameter.get("topic");
		var SCHEMA_FILE = parameter.get("schema-file");
		var DATA_FILE = parameter.get("data-file");

		var schema_string = new String(Files.readAllBytes(Paths.get(SCHEMA_FILE)), StandardCharsets.UTF_8);
		var key_schema = new Schema.Parser().parse(KEY_SCHEMA_STRING);
		var value_schema = new Schema.Parser().parse(schema_string);

		var messages = new ArrayList<String>();
		try (BufferedReader br = new BufferedReader(new FileReader(DATA_FILE))) {
			String line;
			while ((line = br.readLine()) != null) {
				line = line.trim();
				if (line.length() > 0) {
					messages.add(line);
				}
			}
		}

		var props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
		props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY);
		props.put(ProducerConfig.ACKS_CONFIG, "1");

		var decoderFactory = new DecoderFactory();
		try (final KafkaProducer<GenericRecord, GenericRecord> producer = new KafkaProducer<>(props)) {

			for (int i = 0; i < messages.size(); i++) {
				var msg = messages.get(i);
				var decoder = decoderFactory.jsonDecoder(value_schema, msg);
				DatumReader<GenericData.Record> reader = new GenericDatumReader<>(value_schema);
				var value_record = reader.read(null, decoder);

				var key_record = new GenericData.Record(key_schema);
				key_record.put("key", Integer.toString(i));

				producer.send(new ProducerRecord<>(TOPIC, key_record, value_record));
				System.out.println(msg);
			}
		}

	}
}