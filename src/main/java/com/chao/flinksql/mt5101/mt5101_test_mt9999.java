package com.chao.flinksql.mt5101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt5101_test_mt9999 {
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
				"  LASTUPDATEDDT AS PROCTIME()  + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'data-xpq-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'data-xpq-msg-parse-result',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'latest-offset'\n" +
				")");
		
		String msg2 = "{\"Response\":{\"BorderTransportMeans\":{\"JourneyID\":\"835EI\",\"ID\":\"UN9160000\"},\"Consignment\":[{\"AssociatedTransportDocument\":{\"ID\":\"bill10001\"},\"TransportContractDocument\":{\"ID\":\"masterbill10001\"},\"ResponseType\":{\"Text\":\"2313\",\"Code\":\"01\"}}],\"TransportEquipment\":[{\"EquipmentIdentification\":{\"ID\":\"ctnr10001\"},\"TransportContractDocument\":{\"ID\":\"HDM520111315\"},\"ResponseType\":{\"Text\":\"2323\",\"Code\":\"01\"}}]},\"Head\":{\"ReceiverID\":\"2200132208081\",\"SendTime\":\"20211104075450000\",\"Version\":\"1.0\",\"FunctionCode\":\"3\",\"SenderID\":\"2200\",\"MessageType\":\"MT5101\",\"MessageID\":\"SEA_2200132205569_20181004014310534\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"CNC2200\"}}\n";
		
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
				"'null' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg2 + "'" + "as parseData");
	}
}
