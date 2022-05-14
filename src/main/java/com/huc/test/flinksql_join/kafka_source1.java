package com.huc.test.flinksql_join;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class kafka_source1 {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		env.setParallelism(1);
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data1 (\n" +
				"  name STRING,\n" +
				"  id1 STRING,\n" +
				"  id2 STRING,\n" +
				"  ts bigint\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'flink-test-interval1',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'group-flink-test1',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		tEnv.executeSql("" +
				"insert into kafka_source_data1(name,id1,id2,ts) " +
				"select 'lisi' as name," +
				"'a1' as id1," +
				"'b1' as id2," +
				"1650785435000 as ts");
	}
}