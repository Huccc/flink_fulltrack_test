package com.chao.flinksql.mt5102;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt5102_test_mt9999 {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		
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
				"  'properties.group.id' = 'group-flink-msg-mt5102',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		String msg2 = "{\"Response\":{\"BorderTransportMeans\":{\"JourneyID\":\"MT5102001\",\"ID\":\"UN9133329\"},\"Consignment\":[{\"AssociatedTransportDocument\":{\"ID\":\"\"},\"TransportContractDocument\":{\"ID\":\"HDM520111315\"},\"ResponseType\":{\"Text\":\"2313\",\"Code\":\"02\"}}],\"TransportEquipment\":[{\"EquipmentIdentification\":{\"ID\":\"HDM620111315\"},\"TransportContractDocument\":{\"ID\":\"HDM520111315\"},\"ResponseType\":{\"Text\":\"2323\",\"Code\":\"01\"}}]},\"Head\":{\"ReceiverID\":\"2200132208081\",\"SendTime\":\"20221225075456000\",\"Version\":\"1.0\",\"FunctionCode\":\"3\",\"SenderID\":\"2200\",\"MessageType\":\"MT5102\",\"MessageID\":\"MT5102_20210311002\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"CNC2200\"}}\n";
		
//		tEnv.executeSql("" +
//				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
//				"select '1359023762372042843' as msgId," +
//				"'MT5102' as bizId," +
//				"'message_data' as msgType," +
//				"'null' as bizUniqueId," +
//				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
//				"'" + msg1 + "'" + "as parseData");
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT9999' as bizId," +
				"'message_data' as msgType," +
				"'MT9999_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg2 + "'" + "as parseData");
	}
}
