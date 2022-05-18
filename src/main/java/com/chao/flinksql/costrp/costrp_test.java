package com.chao.flinksql.costrp;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class costrp_test {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.setParallelism(1);

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
                "'connector' = 'kafka',\n" +
                "  'topic' = 'source_topic',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'source_topic',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                "  )");

        tEnv.executeSql("" +
                "insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
                "select '1359023762372042843' as msgId," +
                "'COSTRP' as bizId," +
                "'message_data' as msgType," +
                "'COSTRP_TEST' as bizUniqueId," +
                "'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
                "'[{\n" +
                "        \"HeadRecord\":[\n" +
                "            {\n" +
                "                \"FileFunction\":\"7\",\n" +
//                "                \"FileFunction\":\"6\",\n" +
                "                \"FileDescription\":\"6418436816\",\n" +
                "                \"TotalOfContainer\":{\n" +
                "                    \"NumberOfContainers\":\"0\",\n" +
                "                    \"RecordId\":\"20\"\n" +
                "                },\n" +
                "                \"FileCreateTime\":\"20201130142504000\",\n" +
                "                \"EdiCodeOfSender\":\"wgq5\",\n" +
                "                \"RecordId\":\"00\",\n" +
                "                \"CopTo\":[\n" +
                "                    {\n" +
                "                        \"CodeOfCarbonCopyRecipient\":\"13220556-9\",\n" +
                "                        \"RecordId\":\"02\"\n" +
                "                    },\n" +
                "                    {\n" +
                "                        \"CodeOfCarbonCopyRecipient\":\"132208081\",\n" +
                "                        \"RecordId\":\"02\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"ContainerInformation\":[\n" +
                "                    {\n" +
                "                        \"OverWidthLeft\":\"\",\n" +
                "                        \"DateOfPacking\":\"202011290817\",\n" +
                "                        \"OverWidthRight\":\"\",\n" +
                "                        \"AddressOfTheContainerStuffingParty\":\"\",\n" +
                "                        \"ContainerNo\":\"CTN001\",\n" +
                "                        \"ContainerStatus\":\"8\",\n" +
                "                        \"LicensePlateNoOfTrailer\":\"\",\n" +
                "                        \"OverHeight\":\"\",\n" +
                "                        \"BlNo\":[\n" +
                "                            {\n" +
                "                                \"PlaceOfDelivery\":\"NO\",\n" +
                "                                \"CargoInformation\":[\n" +
                "                                    {\n" +
                "                                        \"CargoDescription\":{\n" +
                "                                            \"RecordId\":\"62\",\n" +
                "                                            \"DescriptionOfCargo\":\"NONE\"\n" +
                "                                        },\n" +
                "                                        \"VolumeOfCargo\":\"30.120\",\n" +
                "                                        \"CargoNo\":\"0\",\n" +
                "                                        \"CodeOfPackageType\":\"ZZ\",\n" +
                "                                        \"NumberOfPackages\":\"93\",\n" +
                "                                        \"PackageType\":\"PKG\",\n" +
                "                                        \"GrossWeightOfCargo\":\"19560.000\",\n" +
                "                                        \"CodeOfCargoType\":\"\",\n" +
                "                                        \"RecordId\":\"61\",\n" +
                "                                        \"Marks\":{\n" +
                "                                            \"RecordId\":\"63\",\n" +
                "                                            \"Marks\":\"N/M\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                ],\n" +
                "                                \"PlaceCodeOfDelivery\":\"NO\",\n" +
                "                                \"RecordId\":\"60\",\n" +
                "                                \"Master_BL_No\":\"COSU6283329960\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"TotalNumberOfPackages\":\"0\",\n" +
                "                        \"ContainerSize\":\"22V0\",\n" +
                "                        \"ContainerStuffingParty\":\"\",\n" +
                "                        \"RecordId\":\"50\",\n" +
                "                        \"ContainerOperator\":\"ECC\",\n" +
                "                        \"SealNo\":[\n" +
                "                            {\n" +
                "                                \"SealNo\":\"CM208134\",\n" +
                "                                \"SealType\":\"M\",\n" +
                "                                \"Sealer\":\"SH\",\n" +
                "                                \"RecordId\":\"51\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"LoadDischargePort\":{\n" +
                "                            \"CodeOfCustomsOfDeclaration\":\"2225\",\n" +
                "                            \"PortOfLoading\":\"SHANGHAI\",\n" +
                "                            \"PortCodeOfDischarge\":\"NGTIN\",\n" +
                "                            \"PortOfDestination\":\"\",\n" +
                "                            \"PortCodeOfDestination\":\"\",\n" +
                "                            \"RecordId\":\"52\",\n" +
                "                            \"PortOfDischarge\":\"TINCAN\",\n" +
                "                            \"PortCodeOfLoading\":\"CNSHA\"\n" +
                "                        },\n" +
                "                        \"ContainerTareWeight\":\"2230\",\n" +
                "                        \"InPortInfo\":{\n" +
                "                            \"RecordId\":\"55\",\n" +
                "                            \"CodeOfPort_InLocation\":\"00107/2225\",\n" +
                "                            \"TimeOfPort_In\":\"202011290817\"\n" +
                "                        },\n" +
                "                        \"ContainerOperatorCode\":\"COSCO\",\n" +
                "                        \"PortInMark\":\"Y\",\n" +
                "                        \"TotalWeight\":\"21790\",\n" +
                "                        \"EirNo\":\"\",\n" +
                "                        \"TotalWeightOfGoods\":\"19560.000\",\n" +
                "                        \"ModeOfTransport\":\"\",\n" +
                "                        \"OverLengthFront\":\"\",\n" +
                "                        \"TotalVolume\":\"0.000\",\n" +
                "                        \"OverLengthBack\":\"\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"VslVoyFields\":{\n" +
                "                    \"CarrierName\":\"0ONE\",\n" +
                "                    \"VesselCallSign\":\"3FNW4\",\n" +
                "                    \"NationalityCodeOfShip\":\"PA\",\n" +
                "                    \"CarrierCode\":\"0ONE\",\n" +
                "                    \"LinerType\":\"Y\",\n" +
                "                    \"ImportExportMark\":\"E\",\n" +
                "                    \"Voyage\":\"EE7\",\n" +
                "                    \"RecordId\":\"10\",\n" +
                "                    \"VesselImoNo\":\"UN9302073\",\n" +
                "                    \"VesselName\":\"ALEXANDRIA BRIDGE\"\n" +
                "                },\n" +
                "                \"EdiCodeOfRecipient\":\"EPCMS\",\n" +
                "                \"MessageType\":\"COSTRP\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"Memo\":{\n" +
                "            \"ModificationContactTel\":\"111\",\n" +
                "            \"ModificationContactName\":\"1111\",\n" +
                "            \"RecordId\":\"90\",\n" +
                "            \"ModificationReason\":\"111\",\n" +
                "            \"Memo\":\"111\"\n" +
                "        },\n" +
                "        \"TailRecord\":{\n" +
                "            \"RecordId\":\"99\",\n" +
                "            \"TotalNumberOfRecords\":\"15\"\n" +
                "        }\n" +
                "    }]' as parseData");
        

    }
}
