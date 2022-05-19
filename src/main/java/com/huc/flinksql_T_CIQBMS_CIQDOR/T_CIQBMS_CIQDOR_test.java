package com.huc.flinksql_T_CIQBMS_CIQDOR;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class T_CIQBMS_CIQDOR_test {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO kafka数据源
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS PROCTIME()\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-db-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-tciqbmsciqdor',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'ogg_data' as bizId," +
				"'null' as msgType," +
				"'T_CIQBMS_CIQDOR_test' as bizUniqueId," +
				"'SRC_GCC.T_CIQBMS_CIQDOR' as destination," +
				"'{\"ID\":98308,\"MSGLOGID\":\"31111\",\"MSGTYPE\":null,\"ENTRYID\":null,\"EXAMRECID\":null,\"EXAMMODE\":null,\"IEFLAG\":\"E\",\"VESSEL_NAME\":\"HARRIER HUNTER\",\"VOYAGE\":\"ac8\",\"SUB_BL_NO\":\"100002\",\"BL_NO\":\"master100002\",\"TRADENAME\":null,\"OWNERNAME\":null,\"AGENTNAME\":null,\"DISCHARGE_PLACE\":null,\"CUSTOMS_DISTRICT\":null,\"OUT_DOOR_TIME\":\"2022-05-01 00:01:25\",\"FREEFLAG\":null,\"OP_TYPE\":\"I\",\"F_TAG\":null,\"MATCH_FLAG\":null,\"MSG2DB_TIME\":null,\"CAPXTIMESTAMP\":null,\"CHECKID\":null,\"PARENTLOGID\":\"31111\",\"CTNNO\":\"100003\",\"CAPXTIMESTAMP\":null,\"CHECKTYPE\":null}' as parseData");
	}
}
