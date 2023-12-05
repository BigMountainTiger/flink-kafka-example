package com.song.example;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.song.example.utilities.SchemaRegistry;
import com.song.example.utilities.TypeMap;

public class Consume_Data {

	private static Schema schema;

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);

		Consume_Data.load_schema(parameter);

		final var env = StreamExecutionEnvironment.getExecutionEnvironment();
		var kafka = kafaka(parameter);

		DataStream<GenericRecord> stm = env.fromSource(kafka, WatermarkStrategy.noWatermarks(), "Kafka Source");

		stm = stm.filter(r -> {
			return true;
		});

		var fields = Consume_Data.schema.getFields();
		var size = fields.size();
		final var list = new ArrayList<RowType.RowField>();

		for (int i = 0; i < size; i++) {
			var field = fields.get(i);
			var schema = field.schema();

			// var logicalType = schema.getLogicalType();
			// var type = (logicalType == null) ? schema.getType().name() : logicalType.getName();
			var type = schema.getType().name();
			
			list.add(new RowType.RowField(field.name(), TypeMap.AVRO.get(type)));
		}

		var converter = AvroToRowDataConverters.createRowConverter(new RowType(list));
		var stm_data = stm.map(row -> {
			return (RowData) converter.convert(row);
		});

		System.out.println();
		stm_data.print();

		env.execute("Flink started");
		System.out.println("\nExecution completed");
	}

	private static KafkaSource<GenericRecord> kafaka(ParameterTool parameter) throws IOException {

		String SCHEMA_REGISTRY_URL = parameter.get("schema-registry");
		String SERVER = parameter.get("bootstrp-server");
		String TOPIC = parameter.get("topic");
		String GROUP_ID = "TEST_GROUP_EXAMPLE";

		var schema = Consume_Data.schema;

		KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
				.setTopics(TOPIC)
				.setGroupId(GROUP_ID)
				.setProperty("bootstrap.servers", SERVER)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setBounded(OffsetsInitializer.latest())
				.setValueOnlyDeserializer(
						ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, SCHEMA_REGISTRY_URL))
				.build();

		return source;
	}

	private static void load_schema(ParameterTool parameter) throws IOException {

		String SCHEMA_REGISTRY_URL = parameter.get("schema-registry");
		String TOPIC = parameter.get("topic");

		var sr = new SchemaRegistry(SCHEMA_REGISTRY_URL, TOPIC + "-value");
		var schema = sr.LoadSchema();

		Consume_Data.schema = schema;
	}
}
