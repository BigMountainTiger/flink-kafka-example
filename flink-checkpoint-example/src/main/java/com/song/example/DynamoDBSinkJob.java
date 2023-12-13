// This project use the docker-compose from the 
// https://github.com/BigMountainTiger/docker-common-examples.git

package com.song.example;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.connector.dynamodb.sink.DynamoDbSink;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequest;
import org.apache.flink.connector.dynamodb.sink.DynamoDbWriteRequestType;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.song.example.utilities.SchemaRegistry;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoDBSinkJob {
    private static ParameterTool parameter;
    private static Schema schema;

    public static void main(String[] args) throws Exception {
        parameter = ParameterTool.fromArgs(args);

        schema = SchemaRegistry.LoadSchema(parameter);
        final var env = env();
        final var kafka = kafaka();

        var stm = env.fromSource(kafka, WatermarkStrategy.noWatermarks(), "kafka source");
        stm.print();

        Properties sinkProperties = new Properties();
        sinkProperties.put(AWSConfigConstants.AWS_REGION, "us-east-1");
        sinkProperties.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4566");

        var dynamodb_table_name = "local_dynamodb_table";

        var elementConverter = new ElementConverter<GenericRecord, DynamoDbWriteRequest>() {

            @Override
            public DynamoDbWriteRequest apply(GenericRecord element, Context context) {
                var id = String.valueOf(element.get("id"));
                var operation = String.valueOf(element.get("operation"));

                // Need to take care of possible null values
                var data = (element.get("data") == null) ? null
                        : AttributeValue.fromS(String.valueOf(element.get("data")));

                var type = DynamoDbWriteRequestType.PUT;
                var items = new HashMap<String, AttributeValue>();
                items.put("id", AttributeValue.fromS(id));
                if ("DELETE".equals(operation)) {
                    type = DynamoDbWriteRequestType.DELETE;
                } else {
                    items.put("data", data);
                }

                var request = DynamoDbWriteRequest.builder()
                        .setType(type)
                        .setItem(items)
                        .build();

                return request;
            }
        };

        var dynamodb = DynamoDbSink.<GenericRecord>builder()
                .setDynamoDbProperties(sinkProperties)
                .setTableName(dynamodb_table_name)
                .setOverwriteByPartitionKeys(Arrays.asList("id"))
                .setElementConverter(elementConverter).build();

        stm.sinkTo(dynamodb);
        env.execute("Flink DynamoDB Sink Job");
    }

    private static StreamExecutionEnvironment env() {
        final var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var checkpoint_interval = parameter.getInt("checkpoint-interval");
        var checkpoint_timeout = parameter.getInt("checkpoint-timeout");
        var max_concurrent_checkpoints = parameter.getInt("max-concurrent-checkpoints");
        var checkpoint_dir = parameter.get("checkpoint-dir");

        var config = env.getCheckpointConfig();
        env.enableCheckpointing(checkpoint_interval);
        config.setMinPauseBetweenCheckpoints(checkpoint_interval);
        config.setCheckpointTimeout(checkpoint_timeout);
        config.setMaxConcurrentCheckpoints(max_concurrent_checkpoints);
        config.setCheckpointStorage(checkpoint_dir);

        return env;
    }

    private static KafkaSource<GenericRecord> kafaka() throws IOException {

        var schema_registry_url = parameter.get("schema-registry");
        var bootstrap_server = parameter.get("bootstrap-server");
        var group_id = parameter.get("group-id");
        var topic = parameter.get("topic");

        KafkaSource<GenericRecord> source = KafkaSource.<GenericRecord>builder()
                .setGroupId(group_id)
                .setTopics(topic)
                .setProperty("bootstrap.servers", bootstrap_server)
                .setStartingOffsets(OffsetsInitializer.earliest())
                // .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(
                        ConfluentRegistryAvroDeserializationSchema.forGeneric(schema, schema_registry_url))
                .build();

        return source;
    }

}
