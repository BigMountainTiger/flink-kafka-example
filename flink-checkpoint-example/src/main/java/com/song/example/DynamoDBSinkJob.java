// This project use the docker-compose from the 
// https://github.com/BigMountainTiger/docker-common-examples.git

package com.song.example;

import org.apache.avro.Schema;
import org.apache.flink.api.java.utils.ParameterTool;

import com.song.example.utilities.SchemaRegistry;


// import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DynamoDBSinkJob {
    private static ParameterTool parameter;
    private static Schema schema;
    public static void main(String[] args) throws Exception {
        parameter = ParameterTool.fromArgs(args);

        schema = SchemaRegistry.LoadSchema(parameter);

        System.out.println(schema.getName());

		// final var env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.execute("Flink DynamoDB Sink Job");

        System.out.println("OK");
	}
    
}
