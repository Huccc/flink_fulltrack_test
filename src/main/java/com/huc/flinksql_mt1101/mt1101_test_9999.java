package com.huc.flinksql_mt1101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt1101_test_9999 {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO KAFKA数据源
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS PROCTIME()  + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'mt1101_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt1101_test',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		String msg = "{\"Response\":{\"BorderTransportMeans\":{\"JourneyID\":\"MT1101001\",\"ID\":\"UN9133329\"},\"Consignment\":[{\"AssociatedTransportDocument\":{\"ID\":\"\"},\"TransportContractDocument\":{\"ID\":\"bloo-202101\"},\"ResponseType\":{\"Text\":\"2313\",\"Code\":\"01\"}}],\"TransportEquipment\":[{\"EquipmentIdentification\":{\"ID\":\"HDM620111315\"},\"TransportContractDocument\":{\"ID\":\"bloo-202101\"},\"ResponseType\":{\"Text\":\"2323\",\"Code\":\"01\"}}]},\"Head\":{\"ReceiverID\":\"2200132208081\",\"SendTime\":\"20211104075450000\",\"Version\":\"1.0\",\"FunctionCode\":\"3\",\"SenderID\":\"2200\",\"MessageType\":\"MT1101\",\"MessageID\":\"DET_20210030\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"CNC2200\"}}\n";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT9999' as bizId," +
				"'message_data' as msgType," +
				"'MT9999_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg + "'" + "as parseData");
	}
}
