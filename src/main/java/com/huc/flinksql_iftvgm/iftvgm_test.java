package com.huc.flinksql_iftvgm;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class iftvgm_test {
	public static void main(String[] args) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
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
				"  LASTUPDATEDDT AS PROCTIME()\n" +
				") WITH (\n" +
				"'connector' = 'kafka',\n" +
				"  'topic' = 'IFTVGM2',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'IFTVGM2',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				"  )");
		
		String msg1 = "[{\"VesselVoyageInformation\":{\"VslName\":\"AAL KEMBLA\",\"RecId\":\"11\",\"VslCallSign\":\"9498353\",\"Voyage\":\"001W\"},\"ContainerDetail1\":[{\"SealNo\":\"\",\"WoodenPkgClass\":\"\",\"VgmTime\":\"\",\"PkgQtyInCtnr\":\"\",\"CtnrSizeType\":\"\",\"CargoNetWtInCtnr\":\"\",\"ShipperVgm\":{\"VgmTel\":\"\",\"VgmCntctName\":\"\",\"ShipperName\":\"\",\"ShipperAddrStreet\":\"\",\"VgmOfficer\":\"\",\"VgmDeclAgcy\":\"\",\"VgmMail\":\"\",\"VgmOfficerSign\":\"\",\"VgmCertNo\":\"\",\"VgmEquipmentNo\":\"\",\"RecId\":\"58\",\"Other\":\"\"},\"Remark\":\"\",\"VgmMethod\":\"SM2\",\"CtnrTareWt\":\"\",\"RecId\":\"51\",\"CargoVolumeInCtnr\":\"\",\"VgmLocation\":\"\",\"CtnrNo\":\"CTNR1234567\",\"Vgm\":\"7219\",\"Other\":\"\"}],\"HeadRecord\":{\"FileDesp\":\"\",\"FileFun\":\"9\",\"FileCreateTime\":\"202111131311\",\"RecId\":\"00\",\"MsgType\":\"IFTVGM\",\"SenderCode\":\"703051285\",\"RecipientCode\":\"wgq5\"},\"TrailerRecord\":{\"RecId\":\"99\",\"RecTtlQty\":\"5\"}}]";
		
		String msg2 = "[\n" +
				"    {\n" +
				"        \"VesselVoyageInformation\":{\n" +
				"            \"VslName\":\"AL NEFUD\",\n" +
				"            \"RecId\":\"11\",\n" +
				"            \"VslCallSign\":\"VRQG7\",\n" +
				"            \"Voyage\":\"1713E\"\n" +
				"        },\n" +
				"        \"ContainerDetail1\":[\n" +
				"            {\n" +
				"                \"SealNo\":\"\",\n" +
				"                \"WoodenPkgClass\":\"\",\n" +
				"                \"VgmTime\":\"201703251233\",\n" +
				"                \"PkgQtyInCtnr\":\"\",\n" +
				"                \"CtnrSizeType\":\"40HC\",\n" +
				"                \"CargoNetWtInCtnr\":\"\",\n" +
				"                \"ShipperVgm\":{\n" +
				"                    \"VgmTel\":\"\",\n" +
				"                    \"VgmCntctName\":\"\",\n" +
				"                    \"ShipperName\":\"\",\n" +
				"                    \"ShipperAddrStreet\":\"\",\n" +
				"                    \"VgmOfficer\":\"\",\n" +
				"                    \"VgmDeclAgcy\":\"\",\n" +
				"                    \"VgmMail\":\"\",\n" +
				"                    \"VgmOfficerSign\":\"\",\n" +
				"                    \"VgmCertNo\":\"\",\n" +
				"                    \"VgmEquipmentNo\":\"\",\n" +
				"                    \"RecId\":\"58\",\n" +
				"                    \"Other\":\"\"\n" +
				"                },\n" +
				"                \"Remark\":\"\",\n" +
				"                \"VgmMethod\":\"SM2\",\n" +
				"                \"CtnrTareWt\":\"\",\n" +
				"                \"RecId\":\"51\",\n" +
				"                \"CargoVolumeInCtnr\":\"\",\n" +
				"                \"VgmLocation\":\"\",\n" +
				"                \"CtnrNo\":\"SEGU4824640\",\n" +
				"                \"Vgm\":\"16121\",\n" +
				"                \"Other\":\"\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"HeadRecord\":{\n" +
				"            \"FileDesp\":\"VGM DATA FOR TERMINAL\",\n" +
				"            \"FileFun\":\"9\",\n" +
				"            \"FileCreateTime\":\"201703251236\",\n" +
				"            \"RecId\":\"00\",\n" +
				"            \"MsgType\":\"IFTVGM\",\n" +
				"            \"SenderCode\":\"132204275\",\n" +
				"            \"RecipientCode\":\"74212819-3\"\n" +
				"        },\n" +
				"        \"TrailerRecord\":{\n" +
				"            \"RecId\":\"99\",\n" +
				"            \"RecTtlQty\":\"5\"\n" +
				"        }\n" +
				"    }\n" +
				"]";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select 'IFTVGM测试' as msgId," +
				"'IFTVGM' as bizId," +
				"'message_data' as msgType," +
				"'iftvgm测试' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg1 + "'" + "as parseData");
	}
}
