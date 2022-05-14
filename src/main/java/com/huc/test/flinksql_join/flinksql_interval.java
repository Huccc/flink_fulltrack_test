package com.huc.test.flinksql_join;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class flinksql_interval {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.getConfig().setIdleStateRetentionTime(Time.minutes(0), Time.minutes(10));
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data1 (\n" +
				"  name STRING,\n" +
				"  id1 STRING,\n" +
				"  id2 STRING,\n" +
				"  ts bigint,\n" +
				"  rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
				// 处理时间
				// "row_time AS cast(CURRENT_TIMESTAMP as timestamp(3)), "
				"  watermark for rt as rt - interval '2' second\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'flink-test-interval1',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'group-flink-test1',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data2 (\n" +
				"  score STRING,\n" +
				"  id1 STRING,\n" +
				"  id2 STRING,\n" +
				"  ts bigint,\n" +
				"  rt AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')),\n" +
				"  watermark for rt as rt - interval '2' second\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'flink-test-interval2',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'group-flink-test2',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		Table table1 = tEnv.sqlQuery("select * from kafka_source_data1");
		tEnv.toAppendStream(table1, Row.class).print();
		
		Table table2 = tEnv.sqlQuery("select * from kafka_source_data2");
		tEnv.toAppendStream(table2, Row.class).print();

//		tEnv.executeSql("" +
//				"create view test_join as " +
//				"select name,score,k1.id1 from kafka_source_data1 k1 " +
//				"join kafka_source_data2 k2 " +
//				"on k1.id1=k2.id1");
		
//		tEnv.executeSql("select name,score,k1.id1,k2.id1 from kafka_source_data1 k1 left join kafka_source_data2 k2 on k1.id1=k2.id1");
		
		Table table = tEnv.sqlQuery("select name,score,k1.id1,k2.id1 from kafka_source_data1 k1 left join kafka_source_data2 k2 on k1.id1=k2.id1");
		
		tEnv.toRetractStream(table, Row.class).print();
		env.execute();

//		tEnv.executeSql("create view test_intervaljoin as" +
//				" select name,score,k1.id1,k1.id2 from kafka_source_data1 k1,kafka_source_data2 k2 where k1.id1=k2.id1 and k1.id2=k2.id2" +
//				// k1流保留4s的历史数据和k2流进行join
////				" and k1.rt between k2.rt - interval '4' second and k2.rt");
//				// k1流保留4s的历史数据 并且 k2流保留2s的历史数据
//				" and k1.rt between k2.rt - interval '4' second and k2.rt + interval '2' second");
//
//		Table test_join_table = tEnv.sqlQuery("select * from test_join");
//		tEnv.toAppendStream(test_join_table, Row.class).print();
//
//		env.execute();
	}
}
