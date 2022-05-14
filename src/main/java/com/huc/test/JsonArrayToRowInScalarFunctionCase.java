package com.huc.test;

import com.easipass.flink.table.function.udsf.JsonArrayToRowInCOSTRP;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.junit.Test;

public class JsonArrayToRowInScalarFunctionCase {
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, settings);

    public void execute(String msg) throws Exception {
        DataStream<String> sourceStream = streamEnv.fromElements(msg);
        Table sourceTable = streamTableEnv.fromDataStream(sourceStream, Expressions.$("msg"));

        // register table
        streamTableEnv.createTemporaryView("MyTable", sourceTable);
        // register scalar function
        streamTableEnv.createTemporarySystemFunction("JSONARRAY_TO_ROW_IN_COSTRP", JsonArrayToRowInCOSTRP.class);

        streamTableEnv.executeSql(
                "CREATE VIEW COSTRP_TMP AS " +
                        "SELECT JSONARRAY_TO_ROW_IN_COSTRP(concat('{\"message\":',msg,'}')).message AS Pdata " +
                        "FROM MyTable");
        streamTableEnv.executeSql("" +
                "create view COSTRP as " +
                "select\n" +
                "  HeadRecord,Memo,TailRecord\n" +
                "from (select * from COSTRP_TMP where Pdata is not null) as tempTB1 cross join unnest(Pdata) AS Pdata(HeadRecord,Memo,TailRecord)\n");

        Table table = streamTableEnv.sqlQuery("SELECT HeadRecord FROM COSTRP");
        streamTableEnv.toAppendStream(table, Row.class).print();

        streamEnv.execute();
    }

    @Test
    public void case1() throws Exception {
        String msg = "[\n" +
                "    {\n" +
                "        \"HeadRecord\": [\n" +
                "            {\n" +
                "                \"FileFunction\": \"9\",\n" +
                "                \"FileDescription\": \"文件说明01\",\n" +
                "                \"TotalOfContainer\": {\n" +
                "                    \"NumberOfContainers\": \"0\",\n" +
                "                    \"RecordId\": \"20\"\n" +
                "                },\n" +
                "                \"FileCreateTime\": \"20210315092504123\",\n" +
                "                \"EdiCodeOfSender\": \"wgq5\",\n" +
                "                \"RecordId\": \"00\",\n" +
                "                \"CopTo\": [\n" +
                "                    {\n" +
                "                        \"CodeOfCarbonCopyRecipient\": \"13220556-9\",\n" +
                "                        \"RecordId\": \"02\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"ContainerInformation\": [\n" +
                "                    {\n" +
                "                        \"OverWidthLeft\": \"445.123\",\n" +
                "                        \"DateOfPacking\": \"202103010817\",\n" +
                "                        \"OverWidthRight\": \"332.25\",\n" +
                "                        \"AddressOfTheContainerStuffingParty\": \"上海浦东\",\n" +
                "                        \"ContainerNo\": \"ctnr0315001\",\n" +
                "                        \"ContainerStatus\": \"8\",\n" +
                "                        \"LicensePlateNoOfTrailer\": \"皖A12345\",\n" +
                "                        \"OverHeight\": \"88.254\",\n" +
                "                        \"BlNo\": [\n" +
                "                            {\n" +
                "                                \"PlaceOfDelivery\": \"NONE\",\n" +
                "                                \"CargoInformation\": [\n" +
                "                                    {\n" +
                "                                        \"CargoDescription\": {\n" +
                "                                            \"RecordId\": \"62\",\n" +
                "                                            \"DescriptionOfCargo\": \"NONE\",\n" +
                "                                            \"AdditionalDescriptionOfCargo\": \"2\"\n" +
                "                                        },\n" +
                "                                        \"VolumeOfCargo\": \"8\",\n" +
                "                                        \"CargoNo\": \"1\",\n" +
                "                                        \"CodeOfPackageType\": \"ZZ\",\n" +
                "                                        \"NumberOfPackages\": \"93\",\n" +
                "                                        \"PackageType\": \"PKG\",\n" +
                "                                        \"GrossWeightOfCargo\": \"19560.22345\",\n" +
                "                                        \"CodeOfCargoType\": \"HS001\",\n" +
                "                                        \"RecordId\": \"61\",\n" +
                "                                        \"Marks\": {\n" +
                "                                            \"RecordId\": \"63\",\n" +
                "                                            \"Marks\": \"N/M\"\n" +
                "                                        }\n" +
                "                                    }\n" +
                "                                ],\n" +
                "                                \"PlaceCodeOfDelivery\": \"100\",\n" +
                "                                \"House_BL_No\": \"HOUSBL001\",\n" +
                "                                \"RecordId\": \"60\",\n" +
                "                                \"Master_BL_No\": \"MASBL001\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"TotalNumberOfPackages\": \"1\",\n" +
                "                        \"ContainerSize\": \"UN9501\",\n" +
                "                        \"ContainerStuffingParty\": \"Test001\",\n" +
                "                        \"RecordId\": \"50\",\n" +
                "                        \"ContainerOperator\": \"ECC\",\n" +
                "                        \"SealNo\": [\n" +
                "                            {\n" +
                "                                \"SealNo\": \"CM208134\",\n" +
                "                                \"SealType\": \"M\",\n" +
                "                                \"Sealer\": \"SH\",\n" +
                "                                \"RecordId\": \"51\"\n" +
                "                            }\n" +
                "                        ],\n" +
                "                        \"LoadDischargePort\": {\n" +
                "                            \"CodeOfCustomsOfDeclaration\": \"8\",\n" +
                "                            \"PortOfLoading\": \"SHANGHAI\",\n" +
                "                            \"PortCodeOfDischarge\": \"NGTIN\",\n" +
                "                            \"PortOfDestination\": \"7\",\n" +
                "                            \"PortCodeOfDestination\": \"6\",\n" +
                "                            \"RecordId\": \"52\",\n" +
                "                            \"PortOfDischarge\": \"TINCAN\",\n" +
                "                            \"PortCodeOfLoading\": \"CNSHA\"\n" +
                "                        },\n" +
                "                        \"ContainerTareWeight\": \"2230.123\",\n" +
                "                        \"InPortInfo\": {\n" +
                "                            \"RecordId\": \"55\",\n" +
                "                            \"CodeOfPort_InLocation\": \"12/2021\",\n" +
                "                            \"TimeOfPort_In\": \"202103151038\"\n" +
                "                        },\n" +
                "                        \"ContainerOperatorCode\": \"COSCO\",\n" +
                "                        \"PortInMark\": \"Y\",\n" +
                "                        \"TotalWeight\": \"21790.233\",\n" +
                "                        \"EirNo\": \"EIRNO2021\",\n" +
                "                        \"TotalWeightOfGoods\": \"19560.123\",\n" +
                "                        \"ModeOfTransport\": \"1\",\n" +
                "                        \"OverLengthFront\": \"123.45\",\n" +
                "                        \"TotalVolume\": \"1200.222\",\n" +
                "                        \"OverLengthBack\": \"998.25\"\n" +
                "                    }\n" +
                "                ],\n" +
                "                \"VslVoyFields\": {\n" +
                "                    \"CarrierName\": \"0ONE\",\n" +
                "                    \"VesselCallSign\": \"3FNW4\",\n" +
                "                    \"NationalityCodeOfShip\": \"PA\",\n" +
                "                    \"CarrierCode\": \"0ONE\",\n" +
                "                    \"LinerType\": \"Y\",\n" +
                "                    \"ImportExportMark\": \"E\",\n" +
                "                    \"Voyage\": \"VOY0315008\",\n" +
                "                    \"RecordId\": \"10\",\n" +
                "                    \"VesselImoNo\": \"UN89052320\",\n" +
                "                    \"VesselName\": \"船名001\"\n" +
                "                },\n" +
                "                \"EdiCodeOfRecipient\": \"EPCMS\",\n" +
                "                \"MessageType\": \"COSTRP\"\n" +
                "            }\n" +
                "        ],\n" +
                "        \"Memo\": {\n" +
                "            \"ModificationContactTel\": \"4\",\n" +
                "            \"ModificationContactName\": \"3\",\n" +
                "            \"RecordId\": \"90\",\n" +
                "            \"ModificationReason\": \"2\",\n" +
                "            \"Memo\": \"1\"\n" +
                "        },\n" +
                "        \"TailRecord\": {\n" +
                "            \"RecordId\": \"99\",\n" +
                "            \"TotalNumberOfRecords\": \"15\"\n" +
                "        }\n" +
                "    }\n" +
                "]";
        execute(msg);
    }
}
