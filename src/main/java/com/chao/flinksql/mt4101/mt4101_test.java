package com.chao.flinksql.mt4101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt4101_test {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO Kafka数据源
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
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'group-flink-msg-mt4101',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		String msg1 = "{\"Response\":{\"BorderTransportMeans\":{\"JourneyID\":\"1111315\",\"ID\":\"UN89052320\"},\"Consignment\":[{\"AssociatedTransportDocument\":{\"ID\":\"\"},\"TransportContractDocument\":{\"ID\":\"HDM520111315\"},\"ResponseType\":{\"Text\":\"2313\",\"Code\":\"02\"}}]},\"Head\":{\"ReceiverID\":\"2200132208081\",\"SendTime\":\"20221105075451211\",\"Version\":\"1.0\",\"FunctionCode\":\"3\",\"SenderID\":\"2200\",\"MessageType\":\"MT4101\",\"MessageID\":\"SEA1_2200132238475_2020111315\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"CNC2200\"}}\n";
		
		String msg2 = "{\"Response\":{\"BorderTransportMeans\":{\"JourneyID\":\"111315\",\"ID\":\"UN111315\"},\"Consignment\":[{\"AssociatedTransportDocument\":{\"ID\":\"\"},\"TransportContractDocument\":{\"ID\":\"HDM30111315\"},\"ResponseType\":{\"Text\":\"23删除13\",\"Code\":\"01\"}}]},\"Head\":{\"ReceiverID\":\"2200132208081\",\"SendTime\":\"20201104075460000\",\"Version\":\"1.0\",\"FunctionCode\":\"11\",\"SenderID\":\"2200\",\"MessageType\":\"MT2101\",\"MessageID\":\"SEA_2200132238475_2020111315\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"CNC2200\"}}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT9999' as bizId," +
				"'message_data' as msgType," +
				"'MT4101_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg1 + "'" + "as parseData");
	}
}
