package com.song.example;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

public class Read_Data {

	public static void main(String[] args) throws Exception {
		
		final String input = "/home/song/Sandbox/flink-kafka-example/sink-ouput";
		// final String input = "s3a://huge-head-li-2023-glue-example/output";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Map<String, String> options = new HashMap<>();
		options.put(FlinkOptions.PATH.key(), input);

		HoodiePipeline.Builder builder = HoodiePipeline.builder("t1")
				.column("id INT")
				.column("name VARCHAR(100)")
				.column("updated TIMESTAMP")
				.pk("id")
				.options(options);

		DataStream<RowData> stm = builder.source(env);
		stm.print();

		System.out.println("Start execution");
        env.execute("Flink Streaming");
	}
}