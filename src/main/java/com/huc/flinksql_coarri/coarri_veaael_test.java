package com.huc.flinksql_coarri;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class coarri_veaael_test {
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
				"  LASTUPDATEDDT AS PROCTIME() + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-coarri-customs',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'latest-offset'\n" +
				")");
		
		String msg = "[\n" +
				"    {\n" +
				"        \"VesselVoyageInformation\":{\n" +
				"            \"LoadStartTime\":\"202105201435\",\n" +
				"            \"VslCallSign\":\"DSRJ7\",\n" +
				"            \"Voyage\":\"YA0011\",\n" +
				"            \"DchgStartTime\":\"202011300036\",\n" +
				"            \"EstDeptDate\":\"202011301430\",\n" +
				"            \"ArrivalDate\":\"202011300036\",\n" +
				"            \"VslOprCode\":\"SNKO\",\n" +
				"            \"VslName\":\"9741372\",\n" +
//				"            \"VslName\":\"NAME001\",\n" +
				"            \"IEFlag\":\"E\",\n" +
				"            \"VslNtlCode\":\"KR\",\n" +
				"            \"CtnrQty\":\"632\",\n" +
				"            \"AdPlace\":\"00107\",\n" +
				"            \"LinerTypeCode\":\"7\",\n" +
				"            \"RecId\":\"10\",\n" +
				"            \"LoadCmplTime\":\"202105251435\",\n" +
				"            \"DchgCmplTime\":\"202011301430\"\n" +
				"        },\n" +
				"        \"HeadRecord\":{\n" +
				"            \"FileDesp\":\"MSC MAEVA00\",\n" +
//				"            \"FileDesp\":\"SBARGE\",\n" +
				"            \"FileFun\":\"9\",\n" +
				"            \"FileCreateTime\":\"202103151523\",\n" +
				"            \"RecId\":\"00\",\n" +
				"            \"MsgType\":\"COARRI\",\n" +
				"            \"SenderCode\":\"13220556-9\",\n" +
				"            \"RecipientCode\":\"CNC2200\"\n" +
				"        },\n" +
				"        \"ContainerInformation\":[\n" +
				"            {\n" +
				"                \"CtnrOprName\":\"5\",\n" +
				"                \"SealNo\":\"NONE\",\n" +
				"                \"OverWidthLeft\":\"18\",\n" +
				"                \"CtnrOprCode\":\"SNKO\",\n" +
				"                \"CntrStatusCode\":\"E\",\n" +
				"                \"OverWidthRight\":\"17\",\n" +
				"                \"PkgQtyInCtnr\":\"10\",\n" +
				"                \"CtnrSizeType\":\"45G1\",\n" +
				"                \"CargoNetWtInCtnr\":\"9\",\n" +
				"                \"TeuQty\":\"12\",\n" +
				"                \"BayNo\":\"0100308\",\n" +
				"                \"OverHeight\":\"19\",\n" +
				"                \"OverLengthFront\":\"15\",\n" +
				"                \"RecId\":\"50\",\n" +
				"                \"CargoVolumeInCtnr\":\"11\",\n" +
				"                \"ActualLoadDchgTime\":\"202103251740\",\n" +
				"                \"IntlTsMark\":\"Y\",\n" +
				"                \"CtnrNo\":\"COARRI_0010\",\n" +
				"                \"OverLengthBack\":\"16\"\n" +
				"            },\n" +
				"            {\n" +
				"                \"CtnrOprName\":\"5\",\n" +
				"                \"SealNo\":\"NONE\",\n" +
				"                \"LocationInformation\":{\n" +
				"                    \"DchgPortCode\":\"CNSHA\",\n" +
				"                    \"LoadPortCode\":\"KRPTK\",\n" +
				"                    \"DeliveryPlace\":\"SHANGHAI\",\n" +
				"                    \"CtnrGrossWt\":\"4000\",\n" +
				"                    \"RecId\":\"52\",\n" +
				"                    \"DeliveryPlaceCode\":\"CNSHA\",\n" +
				"                    \"DchgPort\":\"SHANGHAI\",\n" +
				"                    \"LoadPort\":\"PYONGTAEK\"\n" +
				"                },\n" +
				"                \"OverWidthLeft\":\"18\",\n" +
				"                \"CtnrOprCode\":\"SNKO\",\n" +
				"                \"CntrStatusCode\":\"E\",\n" +
				"                \"OverWidthRight\":\"17\",\n" +
				"                \"PkgQtyInCtnr\":\"10\",\n" +
				"                \"CtnrSizeType\":\"45G1\",\n" +
				"                \"CargoNetWtInCtnr\":\"9\",\n" +
				"                \"TeuQty\":\"12\",\n" +
				"                \"BillOfLadingInformation\":[\n" +
				"                    {\n" +
				"                        \"DchgPortCode\":\"CNSHA\",\n" +
				"                        \"BlNo\":\"BL001\",\n" +
				"                        \"LoadPortCode\":\"KRPTK\",\n" +
				"                        \"GrossVolumn\":\"8\",\n" +
				"                        \"RecId\":\"54\",\n" +
				"                        \"CargoDesp\":\"9\",\n" +
				"                        \"TsMark\":\"\",\n" +
				"                        \"PkgQty\":\"140\",\n" +
				"                        \"CargoGrossWt\":\"7\"\n" +
				"                    },\n" +
				"                    {\n" +
				"                        \"DchgPortCode\":\"CNSHA\",\n" +
				"                        \"BlNo\":\"SNK0011\",\n" +
				"                        \"LoadPortCode\":\"KRPTK\",\n" +
				"                        \"GrossVolumn\":\"8\",\n" +
				"                        \"RecId\":\"54\",\n" +
				"                        \"CargoDesp\":\"9\",\n" +
				"                        \"TsMark\":\"\",\n" +
				"                        \"PkgQty\":\"140\",\n" +
				"                        \"CargoGrossWt\":\"7\"\n" +
				"                    }\n" +
				"                ],\n" +
				"                \"BayNo\":\"0100308\",\n" +
				"                \"OverHeight\":\"19\",\n" +
				"                \"OverLengthFront\":\"15\",\n" +
				"                \"RecId\":\"50\",\n" +
				"                \"CargoVolumeInCtnr\":\"11\",\n" +
				"                \"ActualLoadDchgTime\":\"202103251740\",\n" +
				"                \"IntlTsMark\":\"Y\",\n" +
				"                \"CtnrNo\":\"COARRI_0011\",\n" +
				"                \"OverLengthBack\":\"16\"\n" +
				"            }\n" +
				"        ],\n" +
				"        \"TallyCompanyInformation\":{\n" +
				"            \"TallyingOfficer\":\"3\",\n" +
				"            \"Captain/chiefOfficer\":\"4\",\n" +
				"            \"RecId\":\"14\",\n" +
				"            \"TallyingCompany\":\"2\"\n" +
				"        },\n" +
				"        \"TailRecord\":{\n" +
				"            \"RecId\":\"99\",\n" +
				"            \"RecTtlQty\":\"11\"\n" +
				"        }\n" +
				"    }\n" +
				"]";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'COARRI' as bizId," +
				"'message_data' as msgType," +
				"'null' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg + "'" + "as parseData");
	}
}
