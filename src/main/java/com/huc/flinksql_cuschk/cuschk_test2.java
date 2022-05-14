package com.huc.flinksql_cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class cuschk_test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);


        tEnv.executeSql("" +
                "CREATE TABLE source_tb (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'data-xpq-db-parse-result',\n" +
                "  'topic' = 'data-spq-test',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");

        tEnv.executeSql("" +
                "CREATE TABLE sink_tb (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
//                "  'topic' = 'data-xpq-db-parse-result',\n" +
                "  'topic' = 'data-spq-test1',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");

//        tEnv.executeSql("insert into sink_tb select * from source_tb");
        tEnv.executeSql("select * from sink_tb").print();

    }
}
