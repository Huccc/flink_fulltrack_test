package com.chao.flinksql.mt5101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt5101_test_mt5101 {
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
				"  LASTUPDATEDDT AS PROCTIME()  + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'data-xpq-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'data-xpq-msg-parse-result',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'latest-offset'\n" +
				")");
		
		String msg1 = "" +
				"{\n" +
				"    \"Head\":{\n" +
				"        \"ReceiverID\":\"2200\",\n" +
				"        \"SendTime\":\"20181004011631453\",\n" +
				"        \"Version\":\"1.0\",\n" +
				"        \"FunctionCode\":\"2\",\n" +
				"        \"SenderID\":\"2200132205569\",\n" +
				"        \"MessageType\":\"MT5101\",\n" +
				"        \"MessageID\":\"SEA_2200132205569_20181004014310534\"\n" +
				"    },\n" +
				"    \"ExtraInfo\":{\n" +
				"        \"receiver\":\"662445084\",\n" +
				"        \"sender\":\"072926638\"\n" +
				"    },\n" +
				"    \"Declaration\":{\n" +
				"        \"AdditionalInformation\":{\n" +
				"            \"Content\":\"BILL NUMBER 33\"\n" +
				"        },\n" +
				"        \"DeclarationOfficeID\":\"2248\",\n" +
				"        \"TransportEquipment\":[\n" +
				"            {\n" +
				"                \"CharacteristicCode\":\"22G1\",\n" +
				"                \"EquipmentIdentification\":{\n" +
				"                    \"ID\":\"MAEU6959601\"\n" +
				"                },\n" +
				"                \"SealID\":[\n" +
				"                    {\n" +
				"                        \"Value\":\"M/6818755\",\n" +
				"                        \"AgencyCode\":\"AB\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"TransportMeans\":{\n" +
				"                    \"StowageLocationID\":\"0510314\"\n" +
				"                },\n" +
				"                \"FullnessCode\":\"5\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"BorderTransportMeans\":{\n" +
				"            \"ActualDateTime\":\"201810040110086\",\n" +
				"            \"JourneyID\":\"835EI\",\n" +
				"            \"TypeCode\":\"1\",\n" +
				"            \"CompletedDateTime\":\"201810040110086\",\n" +
				"            \"ID\":\"UN9160000\",\n" +
				"            \"Name\":\"CMA CGM ANTOINE DE SAINT EXUPERY\",\n" +
				"            \"UnloadingLocation\":{\n" +
				"                \"ID\":\"00109/2248\"\n" +
				"            }\n" +
				"        },\n" +
				"        \"TallyParty\":{\n" +
				"            \"ID\":\"2200132205569\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT5101' as bizId," +
				"'message_data' as msgType," +
				"'MT5101_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg1 + "'" + "as parseData");
	}
}
