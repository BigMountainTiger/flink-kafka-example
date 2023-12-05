package com.song.example;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresPRECDURESinkJob {

    static class Student {
        final Long id;
        final String Name;
        final Integer Score;
        final Boolean Pass;

        public Student(Long id, String name, Integer score, Boolean pass) {
            this.id = id;
            this.Name = name;
            this.Score = score;
            this.Pass = pass;
        }
    }

    public static void main(String[] args) throws Exception {

        var env = StreamExecutionEnvironment.getExecutionEnvironment();

        var sql = "call public.upsert_student(?, ?, ?, ?);";

        var stm = env.fromElements(
            new Student(1L, "Song Li", 100, true),
            new Student(2L, "Joe Biden", 70, false),
            new Student(3L, "Donald Trump", 60, true)
        );

        stm.addSink(
                JdbcSink.sink(sql,
                        (statement, student) -> {
                            // 1. Index is 1 based
                            // 2. It is possible to call a procedure to update the data
                            statement.setLong(1, student.id);
                            statement.setString(2, student.Name);
                            statement.setInt(3, student.Score);
                            statement.setBoolean(4, student.Pass);
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(1000)
                                .withBatchIntervalMs(200)
                                .withMaxRetries(5)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:postgresql://localhost:5432/postgres")
                                .withDriverName("org.postgresql.Driver")
                                .withUsername("postgres")
                                .withPassword("docker")
                                .build()));

        env.execute();
    }
}
