package com.song.example;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class PostgresSQLSinkJob {

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

        var sql = "insert into public.student values(?, ?, ?, ?) ON CONFLICT (id) "
                + "DO UPDATE SET name = ?, score = ?, pass = ?;";

        var stm = env.fromElements(
            new Student(1L, "Song Li", 100, true),
            new Student(2L, "Joe Biden", 70, false)
        );

        stm.addSink(
                JdbcSink.sink(sql,
                        (statement, student) -> {
                            // 1. Index is 1 based
                            // 2. Postgres driver requires strong type even though postgres takes integer as varchar
                            statement.setLong(1, student.id);
                            statement.setString(2, student.Name);
                            statement.setInt(3, student.Score);
                            statement.setBoolean(4, student.Pass);
                            statement.setString(5, student.Name);
                            statement.setInt(6, student.Score);
                            statement.setBoolean(7, student.Pass);
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
