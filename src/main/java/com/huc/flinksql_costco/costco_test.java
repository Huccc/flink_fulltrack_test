package com.huc.flinksql_costco;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class costco_test {
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
//				"  extraInfo string,\n" +
				"  `proctime` AS PROCTIME()\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'data-xpq-msg-parse-result',\n" +
//                "  'topic' = 'spe_data_sink',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'data-xpq-msg-parse-result',\n" +
//                "  'properties.group.id' = 'spe_data_sink',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'earliest-offset'\n" +
				")");
		
		String msg = "{\n" +
				"    \"HeadRecord\":{\n" +
				"        \"FileFunction\":\"9\",\n" +
				"        \"VesselVoyageInformation\":{\n" +
				"            \"CarrierName\":\"\",\n" +
				"            \"VesselCallSign\":\"OXSQ2\",\n" +
				"            \"NationalityCodeOfShip\":\"\",\n" +
				"            \"CarrierCode\":\"\",\n" +
				"            \"LinerType\":\"\",\n" +
				"            \"ImportExportMark\":\"E\",\n" +
				"            \"Voyage\":\"048W\",\n" +
				"            \"RecordId\":\"10\",\n" +
//				"            \"VesselImoNo\":\"9458092\",\n" +
				"            \"VesselImoNo\":\"89052320\",\n" +
				"            \"VesselName\":\"MAERSK ESSEX\"\n" +
				"        },\n" +
				"        \"FileDescription\":\"EDI\",\n" +
				"        \"CarbonCopyInformation\":[\n" +
				"            {\n" +
				"                \"CodeOfCarbonCopyRecipient\":\"13220556-9\",\n" +
				"                \"RecordId\":\"01\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"NumberOfContainers\":{\n" +
				"            \"NumberOfContainers\":\"1\",\n" +
				"            \"RecordId\":\"20\"\n" +
				"        },\n" +
				"        \"FileCreateTime\":\"202012011442\",\n" +
				"        \"EdiCodeOfSender\":\"072926638\",\n" +
				"        \"RecordId\":\"00\",\n" +
				"        \"ContainerInformation\":[\n" +
				"            {\n" +
				"                \"SealNo\":\"CN7208060\",\n" +
				"                \"ContainerTareWeight\":\"3950\",\n" +
				"                \"OverWidthLeft\":\"\",\n" +
				"                \"PortOfLoadingDischarge\":{\n" +
				"                    \"PortOfLoading\":\"SHANGHAI\",\n" +
				"                    \"PortCodeOfDischarge\":\"LKCMB\",\n" +
				"                    \"RecordId\":\"52\",\n" +
				"                    \"PortOfDischarge\":\"COLOMBO\",\n" +
				"                    \"PortCodeOfLoading\":\"CNSHA\"\n" +
				"                },\n" +
				"                \"DateOfPacking\":\"202112010000\",\n" +
				"                \"OverWidthRight\":\"\",\n" +
				"                \"AddressOfTheContainerStuffingParty\":\"S\",\n" +
				"                \"ContainerNo\":\"TCNU8630230\",\n" +
				"                \"ContainerOperatorCode\":\"MSK\",\n" +
				"                \"BillOfLadingInformation\":[\n" +
				"                    {\n" +
				"                        \"CargoInformation\":[\n" +
				"                            {\n" +
				"                                \"VolumeOfCargo\":\"63.84\",\n" +
				"                                \"CargoNo\":\"1\",\n" +
				"                                \"CodeOfPackageType\":\"S\",\n" +
				"                                \"NumberOfPackages\":\"22\",\n" +
				"                                \"PackageType\":\"\",\n" +
				"                                \"GrossWeightOfCargo\":\"17050\",\n" +
				"                                \"CodeOfCargoType\":\"\",\n" +
				"                                \"CargoMarksInformation\":{\n" +
				"                                    \"RecordId\":\"63\",\n" +
				"                                    \"Marks\":\"S\"\n" +
				"                                },\n" +
				"                                \"RecordId\":\"61\",\n" +
				"                                \"DescriptionOfCargoInformation\":{\n" +
				"                                    \"RecordId\":\"62\",\n" +
				"                                    \"DescriptionOfCargo\":\"S\"\n" +
				"                                }\n" +
				"                            }\n" +
				"                        ],\n" +
				"                        \"MasterBLNo\":\"SGHWB0099\",\n" +
				"                        \"PlaceCodeOfDelivery\":\"S\",\n" +
				"                        \"RecordId\":\"60\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"ContainerStatus\":\"F\",\n" +
				"                \"TotalWeight\":\"21000\",\n" +
				"                \"EirNo\":\"\",\n" +
				"                \"OverHeight\":\"\",\n" +
				"                \"TotalWeightOfGoods\":\"17050\",\n" +
				"                \"ContainerSizeType\":\"45G1\",\n" +
				"                \"OverLengthFront\":\"\",\n" +
				"                \"TotalNumberOfPackages\":\"22\",\n" +
				"                \"TotalVolume\":\"63.84\",\n" +
				"                \"ContainerStuffingParty\":\"S\",\n" +
				"                \"RecordId\":\"50\",\n" +
				"                \"OverLengthBack\":\"\",\n" +
				"                \"ContainerOperator\":\"马士基\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"EdiCodeOfRecipient\":\"662445084\",\n" +
				"        \"MessageType\":\"COSTCO\"\n" +
				"    },\n" +
				"    \"TailRecord\":{\n" +
				"        \"RecordId\":\"99\",\n" +
				"        \"TotalNumberOfRecords\":\"11\"\n" +
				"    }\n" +
				"}";
		
		String str = "{\"HeadRecord\":{" +
				"\"FileFunction\":\"9\"," +
				"\"VesselVoyageInformation\":{" +
				"\"CarrierName\":\"\"," +
				"\"VesselCallSign\":\"OXSQ2\",\"NationalityCodeOfShip\":\"\",\"CarrierCode\":\"\",\"LinerType\":\"\",\"ImportExportMark\":\"E\",\"Voyage\":\"048W\",\"RecordId\":\"10\",\"VesselImoNo\":\"9458092\",\"VesselName\":\"MAERSK ESSEX\"},\"FileDescription\":\"EDI\",\"CarbonCopyInformation\":[{\"CodeOfCarbonCopyRecipient\":\"13220556-9\",\"RecordId\":\"01\"}],\"NumberOfContainers\":{\"NumberOfContainers\":\"1\",\"RecordId\":\"20\"},\"FileCreateTime\":\"202012011442\",\"EdiCodeOfSender\":\"072926638\",\"RecordId\":\"00\",\"ContainerInformation\":[{\"SealNo\":\"CN7208060\",\"ContainerTareWeight\":\"3950\",\"OverWidthLeft\":\"\",\"PortOfLoadingDischarge\":{\"PortOfLoading\":\"SHANGHAI\",\"PortCodeOfDischarge\":\"LKCMB\",\"RecordId\":\"52\",\"PortOfDischarge\":\"COLOMBO\",\"PortCodeOfLoading\":\"CNSHA\"},\"DateOfPacking\":\"202212010000\",\"OverWidthRight\":\"\",\"AddressOfTheContainerStuffingParty\":\"S\",\"ContainerNo\":\"TCNU9730228\",\"ContainerOperatorCode\":\"MSK\",\"BillOfLadingInformation\":[{\"CargoInformation\":[{\"VolumeOfCargo\":\"63.84\",\"CargoNo\":\"1\",\"CodeOfPackageType\":\"S\",\"NumberOfPackages\":\"22\",\"PackageType\":\"\",\"GrossWeightOfCargo\":\"17050\",\"CodeOfCargoType\":\"\",\"CargoMarksInformation\":{\"RecordId\":\"63\",\"Marks\":\"S\"},\"RecordId\":\"61\",\"DescriptionOfCargoInformation\":{\"RecordId\":\"62\",\"DescriptionOfCargo\":\"S\"}}],\"MasterBLNo\":\"SGHWB2097\",\"PlaceCodeOfDelivery\":\"S\",\"RecordId\":\"60\"}],\"ContainerStatus\":\"F\",\"TotalWeight\":\"21000\",\"EirNo\":\"\",\"OverHeight\":\"\",\"TotalWeightOfGoods\":\"17050\",\"ContainerSizeType\":\"45G1\",\"OverLengthFront\":\"\",\"TotalNumberOfPackages\":\"22\",\"TotalVolume\":\"63.84\",\"ContainerStuffingParty\":\"S\",\"RecordId\":\"50\",\"OverLengthBack\":\"\",\"ContainerOperator\":\"马士基\"}],\"EdiCodeOfRecipient\":\"662445084\",\"MessageType\":\"COSTCO\"},\"TailRecord\":{\"RecordId\":\"99\",\"TotalNumberOfRecords\":\"12\"}}";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'COSTCO' as bizId," +
				"'message_data' as msgType," +
				"'631117416193_072926638_662445084_COSTCO_631117416249' as bizUniqueId," +
				"'null' as destination," +
				"'[" + str + "]'" + "as parseData" +
//		        "'{\"receiver\":\"662445084\",\"sender\":\"072926638\",\"messageSubtype\":\"COSTCO_T\"}' as extraInfo" +
				"");

//		tEnv.executeSql("" +
//				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
//				"select '1359023762372042843' as msgId," +
//				"'COSTCO' as bizId," +
//				"'message_data' as msgType," +
//				"'null' as bizUniqueId," +
//				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
//				"'[{\n" +
//				"    \"HeadRecord\":{\n" +
//				"        \"FileFunction\":\"9\",\n" +
//				"        \"VesselVoyageInformation\":{\n" +
//				"            \"CarrierName\":\"\",\n" +
//				"            \"VesselCallSign\":\"OXSQ2\",\n" +
//				"            \"NationalityCodeOfShip\":\"\",\n" +
//				"            \"CarrierCode\":\"\",\n" +
//				"            \"LinerType\":\"\",\n" +
//				"            \"ImportExportMark\":\"E\",\n" +
//				"            \"Voyage\":\"048W\",\n" +
//				"            \"RecordId\":\"10\",\n" +
//				"            \"VesselImoNo\":\"9398450\",\n" +
//				"            \"VesselName\":\"MAERSK ESSEX\"\n" +
//				"        },\n" +
//				"        \"FileDescription\":\"EDI\",\n" +
//				"        \"CarbonCopyInformation\":[\n" +
//				"            {\n" +
//				"                \"CodeOfCarbonCopyRecipient\":\"13220556-9\",\n" +
//				"                \"RecordId\":\"01\"\n" +
//				"            }\n" +
//				"        ],\n" +
//				"        \"NumberOfContainers\":{\n" +
//				"            \"NumberOfContainers\":\"1\",\n" +
//				"            \"RecordId\":\"20\"\n" +
//				"        },\n" +
//				"        \"FileCreateTime\":\"202012011442\",\n" +
//				"        \"EdiCodeOfSender\":\"072926638\",\n" +
//				"        \"RecordId\":\"00\",\n" +
//				"        \"ContainerInformation\":[\n" +
//				"            {\n" +
//				"                \"SealNo\":\"CN7208060\",\n" +
//				"                \"ContainerTareWeight\":\"3950\",\n" +
//				"                \"OverWidthLeft\":\"\",\n" +
//				"                \"PortOfLoadingDischarge\":{\n" +
//				"                    \"PortOfLoading\":\"SHANGHAI\",\n" +
//				"                    \"PortCodeOfDischarge\":\"LKCMB\",\n" +
//				"                    \"RecordId\":\"52\",\n" +
//				"                    \"PortOfDischarge\":\"COLOMBO\",\n" +
//				"                    \"PortCodeOfLoading\":\"CNSHA\"\n" +
//				"                },\n" +
//				"                \"DateOfPacking\":\"202112010000\",\n" +
//				"                \"OverWidthRight\":\"\",\n" +
//				"                \"AddressOfTheContainerStuffingParty\":\"S\",\n" +
//				"                \"ContainerNo\":\"TCNU8630228\",\n" +
//				"                \"ContainerOperatorCode\":\"MSK\",\n" +
//				"                \"BillOfLadingInformation\":[\n" +
//				"                    {\n" +
//				"                        \"CargoInformation\":[\n" +
//				"                            {\n" +
//				"                                \"VolumeOfCargo\":\"63.84\",\n" +
//				"                                \"CargoNo\":\"1\",\n" +
//				"                                \"CodeOfPackageType\":\"S\",\n" +
//				"                                \"NumberOfPackages\":\"22\",\n" +
//				"                                \"PackageType\":\"\",\n" +
//				"                                \"GrossWeightOfCargo\":\"17050\",\n" +
//				"                                \"CodeOfCargoType\":\"\",\n" +
//				"                                \"CargoMarksInformation\":{\n" +
//				"                                    \"RecordId\":\"63\",\n" +
//				"                                    \"Marks\":\"S\"\n" +
//				"                                },\n" +
//				"                                \"RecordId\":\"61\",\n" +
//				"                                \"DescriptionOfCargoInformation\":{\n" +
//				"                                    \"RecordId\":\"62\",\n" +
//				"                                    \"DescriptionOfCargo\":\"S\"\n" +
//				"                                }\n" +
//				"                            }\n" +
//				"                        ],\n" +
//				"                        \"MasterBLNo\":\"SGHWB0097\",\n" +
//				"                        \"PlaceCodeOfDelivery\":\"S\",\n" +
//				"                        \"RecordId\":\"60\"\n" +
//				"                    }\n" +
//				"                ],\n" +
//				"                \"ContainerStatus\":\"F\",\n" +
//				"                \"TotalWeight\":\"21000\",\n" +
//				"                \"EirNo\":\"\",\n" +
//				"                \"OverHeight\":\"\",\n" +
//				"                \"TotalWeightOfGoods\":\"17050\",\n" +
//				"                \"ContainerSizeType\":\"45G1\",\n" +
//				"                \"OverLengthFront\":\"\",\n" +
//				"                \"TotalNumberOfPackages\":\"22\",\n" +
//				"                \"TotalVolume\":\"63.84\",\n" +
//				"                \"ContainerStuffingParty\":\"S\",\n" +
//				"                \"RecordId\":\"50\",\n" +
//				"                \"OverLengthBack\":\"\",\n" +
//				"                \"ContainerOperator\":\"马士基\"\n" +
//				"            }\n" +
//				"        ],\n" +
//				"        \"EdiCodeOfRecipient\":\"662445084\",\n" +
//				"        \"MessageType\":\"COSTCO\"\n" +
//				"    },\n" +
//				"    \"TailRecord\":{\n" +
//				"        \"RecordId\":\"99\",\n" +
//				"        \"TotalNumberOfRecords\":\"11\"\n" +
//				"    }\n" +
//				"}]' as parseData");
	}
}
