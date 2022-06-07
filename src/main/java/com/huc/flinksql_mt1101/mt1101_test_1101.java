package com.huc.flinksql_mt1101;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt1101_test_1101 {
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
		
		String msg = "{\n" +
				"    \"Head\": {\n" +
				"        \"ReceiverID\": \"2200\",\n" +
				"        \"SendTime\": \"20201215104310086\",\n" +
				"        \"Version\": \"1.0\",\n" +
				"        \"FunctionCode\": \"3\",\n" +
				"        \"SenderID\": \"2200132216866\",\n" +
				"        \"MessageType\": \"MT1101\",\n" +
				"        \"MessageID\": \"DET_20210030\"\n" +
				"    },\n" +
				"    \"ExtraInfo\": {\n" +
				"        \"receiver\": \"edi\",\n" +
				"        \"sender\": \"1\"\n" +
				"    },\n" +
				"    \"Declaration\": {\n" +
				"        \"Agent\": {\n" +
				"            \"ID\": \"2200132216866\"\n" +
				"        },\n" +
				"        \"BorderTransportMeans\": {\n" +
				"            \"JourneyID\": \"MT1101001\",\n" +
				"            \"TypeCode\": \"1\",\n" +
				"            \"DepartureDateTime\": \"20201114170000\",\n" +
				"            \"FirstArrivalLocationID\": \"CNSHG\",\n" +
				"            \"ArrivalDateTime\": \"20201219210000086\",\n" +
				"            \"ID\": \"UN9133329\",\n" +
				"            \"Name\": \" test-0100Update \"\n" +
				"        },\n" +
				"        \"Consignment\": [\n" +
				"            {\n" +
				"                \"GoodsConsignedPlace\": {\n" +
				"                    \"ID\": \"QAMES\"\n" +
				"                },\n" +
				"                \"TransportEquipment\":[\n" +
				"                    {\n" +
				"                        \"CharacteristicCode\":\"45G1\",\n" +
				"                        \"EquipmentIdentification\":{\n" +
				"                            \"ID\":\"TRLU7344258\"\n" +
				"                        },\n" +
				"                        \"SealID\":[\n" +
				"                            {\n" +
				"                                \"Value\":\"M/0018161\",\n" +
				"                                \"AgencyCode\":\"SH\"\n" +
				"                            }\n" +
				"                        ],\n" +
				"                        \"SupplierPartyTypeCode\":\"2\",\n" +
				"                        \"FullnessCode\":\"8\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"TransportContractDocument\": {\n" +
				"                    \"ID\": \"bloo-202101\",\n" +
				"                    \"ConditionCode\": \"10\"\n" +
				"                },\n" +
				"                \"Consignor\": {\n" +
				"                    \"Address\": {\n" +
				"                        \"Line\": \"PO BOX 24445, DOHA, QATAR\",\n" +
				"                        \"CountryCode\": \"QA\"\n" +
				"                    },\n" +
				"                    \"Name\": \"QATAR CHEMICAL AND PETROCHEMICAL MARKETING AND DISTRIBUTION COMPANY (M\",\n" +
				"                    \"Communication\": [\n" +
				"                        {\n" +
				"                            \"TypeID\": \"TE\",\n" +
				"                            \"ID\": \"+97440211000\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                },\n" +
				"                \"TotalGrossMassMeasure\": 311086,\n" +
				"                \"ConsignmentPackaging\": {\n" +
				"                    \"QuantityQuantity\": 1,\n" +
				"                    \"TypeCode\": \"PK\"\n" +
				"                },\n" +
				"                \"CustomsStatusCode\": [\n" +
				"                    \"001\"\n" +
				"                ],\n" +
				"                \"Consignee\": {\n" +
				"                    \"Address\": {\n" +
				"                        \"Line\": \"ZHEJIANG HANGZHOU BAY FINE CHEMICAL PARK,SHANGYU 312369,ZHEJIANG CHINA\"\n" +
				"                    },\n" +
				"                    \"Name\": \"TO ORDER\"\n" +
				"                },\n" +
				"                \"NotifyParty\": {\n" +
				"                    \"Address\": {\n" +
				"                        \"Line\": \"ZHEJIANG HANGZHOU BAY FINE CHEMICAL PARK,SHANGYU 312369,ZHEJIANG CHINA\",\n" +
				"                        \"CountryCode\": \"CN\"\n" +
				"                    },\n" +
				"                    \"Name\": \"SINOLIGHT SHAOXING CHEMICALS CO., LTD.\",\n" +
				"                    \"Communication\": [\n" +
				"                        {\n" +
				"                            \"TypeID\": \"TE\",\n" +
				"                            \"ID\": \"0086-571-82833786\"\n" +
				"                        }\n" +
				"                    ]\n" +
				"                },\n" +
				"                \"ConsignmentItem\": [\n" +
				"                    {\n" +
				"                        \"SequenceNumeric\": \"1\",\n" +
				"                        \"ConsignmentItemPackaging\": {\n" +
				"                            \"MarksNumbers\": \"N/M\",\n" +
				"                            \"QuantityQuantity\": 1,\n" +
				"                            \"TypeCode\": \"IB\"\n" +
				"                        },\n" +
				"                        \"Commodity\": {\n" +
				"                            \"CargoDescription\": \"ALPHAPLUS NAO 1-TETRADECENE(C14H28)\"\n" +
				"                        },\n" +
				"                        \"GoodsMeasure\": {\n" +
				"                            \"GrossMassMeasure\": 311086\n" +
				"                        }\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"AssociatedTransportDocument\": {\n" +
				"                    \"ID\": \"\"\n" +
				"                },\n" +
				"                \"LoadingLocation\": {\n" +
				"                    \"LoadingDate\": \"202011141700086\",\n" +
				"                    \"ID\": \"QAMES\"\n" +
				"                },\n" +
				"                \"FreightPayment\": {\n" +
				"                    \"MethodCode\": \"PP\"\n" +
				"                },\n" +
				"                \"UnloadingLocation\": {\n" +
				"                    \"ArrivalDate\": \"20201219\",\n" +
				"                    \"ID\": \"CNSHG/2210\"\n" +
				"                }\n" +
				"            }\n" +
				"        ],\n" +
				"        \"ExitCustomsOffice\": {\n" +
				"            \"ID\": \"QAMES\"\n" +
				"        },\n" +
				"        \"RepresentativePerson\": {\n" +
				"            \"Name\": \"2200132216866\"\n" +
				"        },\n" +
				"        \"Carrier\": {\n" +
				"            \"ID\": \"P0022\"\n" +
				"        }\n" +
				"    }\n" +
				"}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'MT1101' as bizId," +
				"'message_data' as msgType," +
				"'MT1101_TEST' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg + "'" + "as parseData");
	}
}
