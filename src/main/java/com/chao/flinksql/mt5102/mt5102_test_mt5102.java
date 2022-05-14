
package com.chao.flinksql.mt5102;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt5102_test_mt5102 {
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
		
		String msg1 = "{\n" +
				"    \"Head\": {\n" +
				"        \"ReceiverID\": \"2200\",\n" +
				"        \"SendTime\": \"20201213060328146\",\n" +
				"        \"Version\": \"1.0\",\n" +
				"        \"FunctionCode\": \"2\",\n" +
				"        \"SenderID\": \"2200132205569\",\n" +
				"        \"MessageType\": \"MT5102\",\n" +
				"        \"MessageID\": \"MT5102_20210311002\"\n" +
				"    },\n" +
				"    \"ExtraInfo\": {\n" +
				"        \"receiver\": \"edi\",\n" +
				"        \"sender\": \"1\"\n" +
				"    },\n" +
				"    \"Declaration\": {\n" +
				"        \"DeclarationOfficeID\": \"2202\",\n" +
				"        \"BorderTransportMeans\": {\n" +
				"            \"ActualDateTime\": \"202012120212086\",\n" +
				"            \"JourneyID\": \"MT5102001\",\n" +
				"            \"TypeCode\": \"1\",\n" +
				"            \"CompletedDateTime\": \"202204101201271\",\n" +
				"            \"LoadingLocation\": {\n" +
				"                \"ID\": \"00027/2202\"\n" +
				"            },\n" +
				"            \"ID\": \"9133329\",\n" +
				"            \"Name\": \"CHUN JIN\"\n" +
				"        },\n" +
				"        \"Consignment\": [\n" +
				"            {\n" +
				"                \"TransportContractDocument\": {\n" +
				"                    \"ID\": \"C8.5S01\"\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\": 101196,\n" +
				"                \"ConsignmentPackaging\": {\n" +
				"                    \"QuantityQuantity\": 8,\n" +
				"                    \"TypeCode\": \"PK\"\n" +
				"                }\n" +
				"            },\n" +
				"            {\n" +
				"                \"TransportContractDocument\": {\n" +
				"                    \"ID\": \"C8.5S02\"\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\": 720500,\n" +
				"                \"ConsignmentPackaging\": {\n" +
				"                    \"QuantityQuantity\": 94,\n" +
				"                    \"TypeCode\": \"PK\"\n" +
				"                }\n" +
				"            },\n" +
				"            {\n" +
				"                \"TransportContractDocument\": {\n" +
				"                    \"ID\": \"C8.5S03\"\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\": 145844,\n" +
				"                \"ConsignmentPackaging\": {\n" +
				"                    \"QuantityQuantity\": 17,\n" +
				"                    \"TypeCode\": \"PK\"\n" +
				"                }\n" +
				"            }\n" +
				"        ],\n" +
				"        \"TallyParty\": {\n" +
				"            \"ID\": \"2200132205569\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		String msg2 = "{\n" +
				"    \"Head\": {\n" +
				"        \"ReceiverID\": \"2200\",\n" +
				"        \"SendTime\": \"20201213060328146\",\n" +
				"        \"Version\": \"1.0\",\n" +
				"        \"FunctionCode\": \"2\",\n" +
				"        \"SenderID\": \"2200132205569\",\n" +
				"        \"MessageType\": \"MT5102\",\n" +
				"        \"MessageID\": \"MT5102_20210311002\"\n" +
				"    },\n" +
				"    \"ExtraInfo\": {\n" +
				"        \"receiver\": \"edi\",\n" +
				"        \"sender\": \"1\"\n" +
				"    },\n" +
				"    \"Declaration\": {\n" +
				"        \"DeclarationOfficeID\": \"2202\",\n" +
				"        \"BorderTransportMeans\": {\n" +
				"            \"ActualDateTime\": \"202012120212086\",\n" +
				"            \"JourneyID\": \"MT5102001\",\n" +
				"            \"TypeCode\": \"1\",\n" +
//				"            \"CompletedDateTime\": \"202203101201271\",\n" +
				"            \"CompletedDateTime\": \"20220511120127\",\n" +
				"            \"LoadingLocation\": {\n" +
				"                \"ID\": \"00027/2202\"\n" +
				"            },\n" +
				"            \"ID\": \"9133329\",\n" +
				"            \"Name\": \"CHUN JIN\"\n" +
				"        },\n" +
				"        \"Consignment\": [\n" +
				"            {\n" +
				"                \"TransportContractDocument\": {\n" +
				"                    \"ID\": \"C8.5S01\"\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\": 101196,\n" +
				"                \"ConsignmentPackaging\": {\n" +
				"                    \"QuantityQuantity\": 8,\n" +
				"                    \"TypeCode\": \"PK\"\n" +
				"                }\n" +
				"            }\n" +
				"        ],\n" +
				"        \"TallyParty\": {\n" +
				"            \"ID\": \"2200132205569\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		String msg3 = "" +
				"{\n" +
				"    \"Head\":{\n" +
				"        \"ReceiverID\":\"2200\",\n" +
				"        \"SendTime\":\"20180925155924381\",\n" +
				"        \"Version\":\"1.0\",\n" +
				"        \"FunctionCode\":\"2\",\n" +
				"        \"SenderID\":\"2200736655771\",\n" +
				"        \"MessageType\":\"MT5102\",\n" +
				"        \"MessageID\":\"SEA_2200736655771_20180925155925837\"\n" +
				"    },\n" +
				"    \"ExtraInfo\":{\n" +
				"        \"receiver\":\"662445084\",\n" +
				"        \"sender\":\"072926638\"\n" +
				"    },\n" +
				"    \"Declaration\":{\n" +
				"        \"DeclarationOfficeID\":\"2247\",\n" +
				"        \"BorderTransportMeans\":{\n" +
				"            \"ActualDateTime\":\"20180925080000\",\n" +
				"            \"JourneyID\":\"331\",\n" +
				"            \"TypeCode\":\"1\",\n" +
				"            \"CompletedDateTime\":\"20180925150000\",\n" +
				"            \"LoadingLocation\":{\n" +
				"                \"ID\":\"CNJNS/2247\"\n" +
				"            },\n" +
				"            \"ID\":\"UN9276145\",\n" +
				"            \"Name\":\"STOLT DISTRIBUTOR\"\n" +
				"        },\n" +
				"        \"Consignment\":[\n" +
				"            {\n" +
				"                \"TransportContractDocument\":{\n" +
				"                    \"ID\":\"SSAGA059HBR800\"\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\":2996743,\n" +
				"                \"ConsignmentPackaging\":{\n" +
				"                    \"QuantityQuantity\":1,\n" +
				"                    \"TypeCode\":\"PS\"\n" +
				"                }\n" +
				"            }\n" +
				"        ],\n" +
				"        \"TallyParty\":{\n" +
				"            \"ID\":\"2200736655771\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT5102' as bizId," +
				"'message_data' as msgType," +
				"'null' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg2 + "'" + "as parseData");

	}
}
