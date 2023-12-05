package com.song.example;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import com.song.example.utilities.SchemaRegistry;

public class Sink_Data {

	private static Schema schema;

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterTool.fromArgs(args);

		final String output = "/home/song/Sandbox/flink-kafka-example/sink-ouput";
		// final String output = "s3a://huge-head-li-2023-glue-example/output";

		FileUtils.deleteDirectory(new File(output));

		Sink_Data.load_schema(parameter);

		final var env = StreamExecutionEnvironment.getExecutionEnvironment();
		var kafka = kafaka(parameter);

		DataStream<GenericRecord> stm = env.fromSource(kafka, WatermarkStrategy.noWatermarks(), "Kafka Source");

		var stm_data = stm.map(r -> {
			var fields = r.getSchema().getFields();
			var size = fields.size();
			var data = new GenericRowData(size);

			data.setField(0, r.get(0));
			data.setField(1, StringData.fromString(r.get(1).toString()));
			data.setField(2, TimestampData.fromEpochMillis((long) r.get(2)));

			return (RowData) data;
		});

		stm_data.print();

		Map<String, String> options = new HashMap<>();
		options.put(FlinkOptions.PATH.key(), output);

		HoodiePipeline.Builder builder = HoodiePipeline.builder("t1")
				.column("id INT")
				.column("name VARCHAR(100)")
				.column("updated TIMESTAMP")
				.pk("id")
				.options(options);

		builder.sink(stm_data, true);

		env.execute("Sink_data");
	}

	private static KafkaSource<GenericRecord> kafaka(ParameterTool parameter) throws IOException {

		String SCHEMA_REGISTRY_URL = parameter.get("schema-registry");
		String SERVER = parameter.get("bootstrp-server");
		String TOPIC = parameter.get("topic");

		var schema = Sink_Data.schema;

		KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
				.setTopics(TOPIC)
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

		Sink_Data.schema = schema;
	}
}