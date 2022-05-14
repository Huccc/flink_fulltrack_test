package com.huc.flinksql_cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class cuschk3 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

        tEnv.executeSql("" +
                "CREATE TABLE KAFKA_DATA_XPQ_DB_PARSE_RESULT (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING,\n" +
                "  msgType STRING,\n" +
                "  bizUniqueId STRING,\n" +
                "  destination STRING,\n" +
                "  parseData STRING -- 此处是 JSON 字符串，需要使用 UDF 进行转换\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'data-xpq-db-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
//                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");

        tEnv.executeSql("select msgId,bizId from KAFKA_DATA_XPQ_DB_PARSE_RESULT").print();
    }
}
