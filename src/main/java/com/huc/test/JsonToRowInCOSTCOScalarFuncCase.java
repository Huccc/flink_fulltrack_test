package com.huc.test;

import com.easipass.flink.table.function.udsf.JsonToRowInCOSTCO;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class JsonToRowInCOSTCOScalarFuncCase {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, settings);

    public void execute(String msg) throws Exception {
        String newMsg = "{\"Msg\":"+ msg +"}";
        DataStream<String> sourceStream = streamEnv.fromElements(newMsg);
        Table sourceTable = streamTableEnv.fromDataStream(sourceStream, Expressions.$("msg"));

        // register table
        streamTableEnv.createTemporaryView("MyTable", sourceTable);
        // register scalar function
        streamTableEnv.createTemporarySystemFunction("JSON_TO_ROW_IN_COSTCO", JsonToRowInCOSTCO.class);

        streamTableEnv.executeSql(
                "CREATE VIEW COSTCO AS " +
                        "SELECT JSON_TO_ROW_IN_COSTCO(msg) AS COSTCO " +
                        "FROM MyTable");

        Table table = streamTableEnv.sqlQuery("SELECT * FROM COSTCO");
        streamTableEnv.toAppendStream(table, Row.class).print();

        streamEnv.execute();
    }

    @Test
    public void case1() throws Exception {
        String msgCOSTCO =
                "[{\n" +
                        "        \"HeadRecord\":{\n" +
                        "            \"FileFunction\":\"9\",\n" +
                        "            \"VesselVoyageInformation\":{\n" +
                        "                \"CarrierName\":\"\",\n" +
                        "                \"VesselCallSign\":\"OXSQ2\",\n" +
                        "                \"NationalityCodeOfShip\":\"\",\n" +
                        "                \"CarrierCode\":\"\",\n" +
                        "                \"LinerType\":\"\",\n" +
                        "                \"ImportExportMark\":\"E\",\n" +
                        "                \"Voyage\":\"048W\",\n" +
                        "                \"RecordId\":\"10\",\n" +
                        "                \"VesselImoNo\":\"9458092\",\n" +
                        "                \"VesselName\":\"MAERSK ESSEX\"\n" +
                        "            },\n" +
                        "            \"FileDescription\":\"EDI\",\n" +
                        "            \"CarbonCopyInformation\":[\n" +
                        "                {\n" +
                        "                    \"CodeOfCarbonCopyRecipient\":\"13220556-9\",\n" +
                        "                    \"RecordId\":\"01\"\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"NumberOfContainers\":{\n" +
                        "                \"NumberOfContainers\":\"1\",\n" +
                        "                \"RecordId\":\"20\"\n" +
                        "            },\n" +
                        "            \"FileCreateTime\":\"202012011442\",\n" +
                        "            \"EdiCodeOfSender\":\"072926638\",\n" +
                        "            \"RecordId\":\"00\",\n" +
                        "            \"ContainerInformation\":[\n" +
                        "                {\n" +
                        "                    \"SealNo\":\"CN7208060\",\n" +
                        "                    \"ContainerTareWeight\":\"3950\",\n" +
                        "                    \"OverWidthLeft\":\"\",\n" +
                        "                    \"PortOfLoadingDischarge\":{\n" +
                        "                        \"PortOfLoading\":\"SHANGHAI\",\n" +
                        "                        \"PortCodeOfDischarge\":\"LKCMB\",\n" +
                        "                        \"RecordId\":\"52\",\n" +
                        "                        \"PortOfDischarge\":\"COLOMBO\",\n" +
                        "                        \"PortCodeOfLoading\":\"CNSHA\"\n" +
                        "                    },\n" +
                        "                    \"DateOfPacking\":\"202012010000\",\n" +
                        "                    \"OverWidthRight\":\"\",\n" +
                        "                    \"AddressOfTheContainerStuffingParty\":\"S\",\n" +
                        "                    \"ContainerNo\":\"TCNU8630228\",\n" +
                        "                    \"ContainerOperatorCode\":\"MSK\",\n" +
                        "                    \"BillOfLadingInformation\":[\n" +
                        "                        {\n" +
                        "                            \"CargoInformation\":[\n" +
                        "                                {\n" +
                        "                                    \"VolumeOfCargo\":\"63.84\",\n" +
                        "                                    \"CargoNo\":\"1\",\n" +
                        "                                    \"CodeOfPackageType\":\"S\",\n" +
                        "                                    \"NumberOfPackages\":\"22\",\n" +
                        "                                    \"PackageType\":\"\",\n" +
                        "                                    \"GrossWeightOfCargo\":\"17050\",\n" +
                        "                                    \"CodeOfCargoType\":\"\",\n" +
                        "                                    \"CargoMarksInformation\":{\n" +
                        "                                        \"RecordId\":\"63\",\n" +
                        "                                        \"Marks\":\"S\"\n" +
                        "                                    },\n" +
                        "                                    \"RecordId\":\"61\",\n" +
                        "                                    \"DescriptionOfCargoInformation\":{\n" +
                        "                                        \"RecordId\":\"62\",\n" +
                        "                                        \"DescriptionOfCargo\":\"S\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ],\n" +
                        "                            \"MasterBLNo\":\"SGHWB0097\",\n" +
                        "                            \"PlaceCodeOfDelivery\":\"S\",\n" +
                        "                            \"RecordId\":\"60\"\n" +
                        "                        }\n" +
                        "                    ],\n" +
                        "                    \"ContainerStatus\":\"F\",\n" +
                        "                    \"TotalWeight\":\"21000\",\n" +
                        "                    \"EirNo\":\"\",\n" +
                        "                    \"OverHeight\":\"\",\n" +
                        "                    \"TotalWeightOfGoods\":\"17050\",\n" +
                        "                    \"ContainerSizeType\":\"45G1\",\n" +
                        "                    \"OverLengthFront\":\"\",\n" +
                        "                    \"TotalNumberOfPackages\":\"22\",\n" +
                        "                    \"TotalVolume\":\"63.84\",\n" +
                        "                    \"ContainerStuffingParty\":\"S\",\n" +
                        "                    \"RecordId\":\"50\",\n" +
                        "                    \"OverLengthBack\":\"\",\n" +
                        "                    \"ContainerOperator\":\"马士基\"\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"EdiCodeOfRecipient\":\"662445084\",\n" +
                        "            \"MessageType\":\"COSTCO\"\n" +
                        "        },\n" +
                        "        \"TailRecord\":{\n" +
                        "            \"RecordId\":\"99\",\n" +
                        "            \"TotalNumberOfRecords\":\"11\"\n" +
                        "        }\n" +
                        "    },\n" +
                        "    {\n" +
                        "        \"HeadRecord\":{\n" +
                        "            \"FileFunction\":\"9\",\n" +
                        "            \"VesselVoyageInformation\":{\n" +
                        "                \"CarrierName\":\"\",\n" +
                        "                \"VesselCallSign\":\"OXSQ2\",\n" +
                        "                \"NationalityCodeOfShip\":\"\",\n" +
                        "                \"CarrierCode\":\"\",\n" +
                        "                \"LinerType\":\"\",\n" +
                        "                \"ImportExportMark\":\"E\",\n" +
                        "                \"Voyage\":\"048W\",\n" +
                        "                \"RecordId\":\"10\",\n" +
                        "                \"VesselImoNo\":\"9458092\",\n" +
                        "                \"VesselName\":\"MAERSK ESSEX\"\n" +
                        "            },\n" +
                        "            \"FileDescription\":\"EDI\",\n" +
                        "            \"CarbonCopyInformation\":[\n" +
                        "                {\n" +
                        "                    \"CodeOfCarbonCopyRecipient\":\"13220556-9\",\n" +
                        "                    \"RecordId\":\"01\"\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"NumberOfContainers\":{\n" +
                        "                \"NumberOfContainers\":\"1\",\n" +
                        "                \"RecordId\":\"20\"\n" +
                        "            },\n" +
                        "            \"FileCreateTime\":\"202012011442\",\n" +
                        "            \"EdiCodeOfSender\":\"072926638\",\n" +
                        "            \"RecordId\":\"00\",\n" +
                        "            \"ContainerInformation\":[\n" +
                        "                {\n" +
                        "                    \"SealNo\":\"CN7208060\",\n" +
                        "                    \"ContainerTareWeight\":\"3950\",\n" +
                        "                    \"OverWidthLeft\":\"\",\n" +
                        "                    \"PortOfLoadingDischarge\":{\n" +
                        "                        \"PortOfLoading\":\"SHANGHAI\",\n" +
                        "                        \"PortCodeOfDischarge\":\"LKCMB\",\n" +
                        "                        \"RecordId\":\"52\",\n" +
                        "                        \"PortOfDischarge\":\"COLOMBO\",\n" +
                        "                        \"PortCodeOfLoading\":\"CNSHA\"\n" +
                        "                    },\n" +
                        "                    \"DateOfPacking\":\"202012010000\",\n" +
                        "                    \"OverWidthRight\":\"\",\n" +
                        "                    \"AddressOfTheContainerStuffingParty\":\"S\",\n" +
                        "                    \"ContainerNo\":\"TCNU8630228\",\n" +
                        "                    \"ContainerOperatorCode\":\"MSK\",\n" +
                        "                    \"BillOfLadingInformation\":[\n" +
                        "                        {\n" +
                        "                            \"CargoInformation\":[\n" +
                        "                                {\n" +
                        "                                    \"VolumeOfCargo\":\"63.84\",\n" +
                        "                                    \"CargoNo\":\"1\",\n" +
                        "                                    \"CodeOfPackageType\":\"S\",\n" +
                        "                                    \"NumberOfPackages\":\"22\",\n" +
                        "                                    \"PackageType\":\"\",\n" +
                        "                                    \"GrossWeightOfCargo\":\"17050\",\n" +
                        "                                    \"CodeOfCargoType\":\"\",\n" +
                        "                                    \"CargoMarksInformation\":{\n" +
                        "                                        \"RecordId\":\"63\",\n" +
                        "                                        \"Marks\":\"S\"\n" +
                        "                                    },\n" +
                        "                                    \"RecordId\":\"61\",\n" +
                        "                                    \"DescriptionOfCargoInformation\":{\n" +
                        "                                        \"RecordId\":\"62\",\n" +
                        "                                        \"DescriptionOfCargo\":\"S\"\n" +
                        "                                    }\n" +
                        "                                }\n" +
                        "                            ],\n" +
                        "                            \"MasterBLNo\":\"SGHWB0097\",\n" +
                        "                            \"PlaceCodeOfDelivery\":\"S\",\n" +
                        "                            \"RecordId\":\"60\"\n" +
                        "                        }\n" +
                        "                    ],\n" +
                        "                    \"ContainerStatus\":\"F\",\n" +
                        "                    \"TotalWeight\":\"21000\",\n" +
                        "                    \"EirNo\":\"\",\n" +
                        "                    \"OverHeight\":\"\",\n" +
                        "                    \"TotalWeightOfGoods\":\"17050\",\n" +
                        "                    \"ContainerSizeType\":\"45G1\",\n" +
                        "                    \"OverLengthFront\":\"\",\n" +
                        "                    \"TotalNumberOfPackages\":\"22\",\n" +
                        "                    \"TotalVolume\":\"63.84\",\n" +
                        "                    \"ContainerStuffingParty\":\"S\",\n" +
                        "                    \"RecordId\":\"50\",\n" +
                        "                    \"OverLengthBack\":\"\",\n" +
                        "                    \"ContainerOperator\":\"马士基\"\n" +
                        "                }\n" +
                        "            ],\n" +
                        "            \"EdiCodeOfRecipient\":\"662445084\",\n" +
                        "            \"MessageType\":\"COSTCO\"\n" +
                        "        },\n" +
                        "        \"TailRecord\":{\n" +
                        "            \"RecordId\":\"99\",\n" +
                        "            \"TotalNumberOfRecords\":\"11\"\n" +
                        "        }\n" +
                        "    }]" ;
        execute(msgCOSTCO);
    }
}
