package com.chao.flinksql.departure;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class departure_test {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'DYN_DEP_DECLARE',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'DYN_DEP_DECLARE',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'ogg_data' as bizId," +
				"'DECLARE_TEST' as msgType," +
				"'LEAVE_PORT' as bizUniqueId," +
				"'SRC_SHIPDYN.DYN_DEP_DECLARE' as destination," +
				"'{\"ID\":98308,\"BUSSINESS_TYPE\":\"31111\",\"VESSELNAME_EN\":\"CHUN JINN\",\"IMO_NO\":\"91131733\",\"VESSEL_CALL\":\"3FOW3\",\"VOYAGE_IN\":\"YA0012\",\"DEPARTURE_DATE\":\"2022-04-08 00:01:25\",\"VOYAGE_OUT\":\"YA0011\",\"BUSSINESS_TYPE\":\"31111\",\"VESSELNAME_CN\":\"亿通国际\"}' as parseData");
	}
}
