package com.huc.flinksql_mt3102;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt3102_test_mt3102 {
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
				"  'topic' = 'mt3102_source',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt3102_source',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		String msg = "{\n" +
				"    \"Head\":{\n" +
				"        \"ReceiverID\":\"2200\",\n" +
				"        \"SendTime\":\"20201221015133379\",\n" +
				"        \"Version\":\"1.0\",\n" +
				"        \"FunctionCode\":\"2\",\n" +
				"        \"SenderID\":\"2200631423296\",\n" +
				"        \"MessageType\":\"MT3102\",\n" +
//				"        \"MessageID\":\"SEA_2200631423296_20201221015221001\"\n" +
				"        \"MessageID\":\"DSEA_2200631423296_20201221015221001\"\n" +
				"    },\n" +
				"    \"ExtraInfo\":{\n" +
				"        \"receiver\":\"edi\",\n" +
				"        \"sender\":\"1\"\n" +
				"    },\n" +
				"    \"Declaration\":{\n" +
				"        \"DeclarationOfficeID\":\"2248\",\n" +
				"        \"TransportEquipment\":[\n" +
				"            {\n" +
				"                \"CharacteristicCode\":\"22G1\",\n" +
				"                \"EquipmentIdentification\":{\n" +
				"                    \"ID\":\"CAAU2113406\"\n" +
				"                },\n" +
				"                \"SealID\":[\n" +
				"                    {\n" +
				"                        \"Value\":\"M/1386439\",\n" +
				"                        \"AgencyCode\":\"AB\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"FullnessCode\":\"5\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"BorderTransportMeans\":{\n" +
				"            \"JourneyID\":\"002E\",\n" +
				"            \"TypeCode\":\"1\",\n" +
				"            \"ID\":\"UN89052320\",\n" +
				"            \"Name\":\"HMM COPENHAGEN\"\n" +
				"        },\n" +
				"        \"UnloadingLocation\":{\n" +
				"            \"ArrivalDate\":\"20201221\",\n" +
				"            \"ID\":\"48001/2248\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT3102' as bizId," +
				"'message_data' as msgType," +
				"'MT3102_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg + "'" + "as parseData");
		
	}
}
