package com.huc.flinksql_mt3101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt3101_test_3101 {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO KAFKA数据源
		tEnv.executeSql("" +
				"CREATE TABLE DATA_XPQ_MSG_PARSE_RESULT (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'mt3101',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		String msg2101 = "{\"Head\":{\"ReceiverID\":\"2200\",\"SendTime\":\"20200330132817685\",\"Version\":\"1.0\",\"FunctionCode\":\"9\",\"SenderID\":\"2200350816456\",\"MessageType\":\"MT2101\",\"MessageID\":\"SEA_2200132238475_2020111317\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"132208081\"},\"Declaration\":{\"Agent\":{\"ID\":\"2200350816456\"},\"BorderTransportMeans\":{\"JourneyID\":\"MT1101001\",\"TypeCode\":\"1\",\"ID\":\"UN9133329\",\"Name\":\"MSC OLIVER\"},\"Consignment\":[{\"GoodsConsignedPlace\":{\"ID\":\"CN\"},\"GoodsReceiptPlace\":{\"ID\":\"ESBCN\",\"Name\":\"BARCELONA\"},\"TransportEquipment\":[{\"CharacteristicCode\":\"45G1\",\"EquipmentIdentification\":{\"ID\":\"TCNU2865375\"},\"FullnessCode\":\"5\"}],\"TransportContractDocument\":{\"ID\":\"HDM20111318\",\"ConditionCode\":\"11\"},\"AssociatedTransportDocument\":{\"ID\":\"2101\",\"ConditionCode\":\"11\"},\"Consignor\":{\"Address\":{\"Line\":\"HAILUN CENTER,440 HAILUN RD.HONGKOU\",\"CountryCode\":\"CN\"},\"Name\":\"HELLMANN WORLDWIDE LOGISTICS(CHINA)\",\"Communication\":[{\"TypeID\":\"TE\",\"ID\":\"26100260\"}]},\"TotalGrossMassMeasure\":5600.0000,\"ConsignmentPackaging\":{\"QuantityQuantity\":610,\"TypeCode\":\"PK\"},\"CustomsStatusCode\":[\"001\"],\"Consignee\":{\"Address\":{\"Line\":\"POL. IND. MAS BLAU II\",\"CountryCode\":\"ES\"},\"Name\":\"HELLMANN WORLDWIDE LOGISTICS, S.A.\",\"Communication\":[{\"TypeID\":\"TE\",\"ID\":\"+34 93 2643870\"}]},\"ConsignmentItem\":[{\"SequenceNumeric\":\"1\",\"ConsignmentItemPackaging\":{\"MarksNumbers\":\"N/M\",\"QuantityQuantity\":610,\"TypeCode\":\"PK\"},\"Commodity\":{\"CargoDescription\":\"SLIPPERS\"},\"GoodsMeasure\":{\"GrossMassMeasure\":5600.0000}}],\"NotifyParty\":{\"Address\":{\"Line\":\"A\",\"CountryCode\":\"ES\"},\"Name\":\"HELLMANN WORLDWIDE LOGISTICS, S.A.\",\"Communication\":[{\"TypeID\":\"TE\",\"ID\":\"65953888\"}]},\"LoadingLocation\":{\"LoadingDate\":\"202004171600086\",\"ID\":\"CNYSA/2248\"},\"FreightPayment\":{\"MethodCode\":\"CC\"},\"UnloadingLocation\":{\"ID\":\"ESBCN\"}}],\"ExitCustomsOffice\":{\"ID\":\"XXXXX\"},\"RepresentativePerson\":{\"Name\":\"2200350816456\"},\"Carrier\":{\"ID\":\"MSK\"}}}";
		
		String msg3101 = "{\"Head\":{\"ReceiverID\":\"2200\",\"SendTime\":\"20181006164847410\",\"Version\":\"1.0\",\"FunctionCode\":\"2\",\"SenderID\":\"2200736655771\",\"MessageType\":\"MT3101\",\"MessageID\":\"SEA1_2200132238475_2020111315\"},\"ExtraInfo\":{\"receiver\":\"edi\",\"sender\":\"132208081\"},\"Declaration\":{\"DeclarationOfficeID\":\"2247\",\"BorderTransportMeans\":{\"JourneyID\":\"MT3101001\",\"TypeCode\":\"1\",\"ID\":\"UN9133329\",\"Name\":\"STOLT SUISEN\"},\"Consignment\":[{\"ConsignmentItem\":[{\"SequenceNumeric\":\"1\",\"Commodity\":{\"CargoDescription\":\"1 IN BULK DESMODUR 44V20L\"}}],\"TransportContractDocument\":{\"ID\":\"HDM20111318\"},\"AssociatedTransportDocument\":{\"ID\":\"3101\"},\"TotalGrossMassMeasure\":4000000,\"ConsignmentPackaging\":{\"QuantityQuantity\":1,\"TypeCode\":\"PS\"}}],\"TransportEquipment\":[{\"CharacteristicCode\":\"45G1\",\"EquipmentIdentification\":{\"ID\":\"HDM620111315\"},\"FullnessCode\":\"5\"}],\"UnloadingLocation\":{\"ArrivalDate\":\"20181006\",\"ID\":\"CNJNS/2247\"}}}";
		
		tEnv.executeSql("" +
				"insert into DATA_XPQ_MSG_PARSE_RESULT(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT3101' as bizId," +
				"'message_data' as msgType," +
				"'MT3101_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg3101 + "'" + "as parseData");
	}
}
