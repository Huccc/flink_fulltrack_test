package com.huc.flinksql_cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class letpas2 {
    public static void main(String[] args) throws Exception {
        // TODO:1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);
        // TODO:2.获取数据源
        TableResult tableResult = tEnv.executeSql("" +
                "CREATE TABLE sourceTB (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING,\n" +
                "  msgType STRING,\n" +
                "  bizUniqueId STRING,\n" +
                "  destination STRING,\n" +
                "  parseData STRING,\n" +
                "  LASTUPDATEDDT AS PROCTIME() + INTERVAL '8' HOUR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-bdpcollect-db-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-letpas',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'group-offsets'\n" +
                ")");

        tEnv.executeSql("select * from sourceTB").print();

        env.execute();
    }
}






