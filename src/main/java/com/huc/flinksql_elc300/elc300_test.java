package com.huc.flinksql_elc300;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class elc300_test {
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
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'source_topic',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'source_topic',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'json'\n" +
				")");
		
		String msg = "{\n" +
				"    \"DataInfo\":{\n" +
				"        \"DockInfoList\":[\n" +
				"            {\n" +
				"                \"DockInfo\":{\n" +
				"                    \"SHIP_NAME_CN\":\"秋锦\",\n" +
				"                    \"TRAF_MODE\":\"2\",\n" +
				"                    \"NODE_3\":\"\",\n" +
				"                    \"NODE_2\":\"\",\n" +
				"                    \"OPER_TIME\":\"20191108152720\",\n" +
				"                    \"SHIP_NAME_EN\":\"QIU JIN\",\n" +
				"                    \"NODE_1\":\"\",\n" +
				"                    \"STATUS_CODE\":\"LIFTOFF\",\n" +
				"                    \"AREA_CODE\":\"310000\",\n" +
				"                    \"CNTR_TYPE\":\"H1\",\n" +
				"                    \"BL_NO\":\"JJHOSSHWNC9Y144\",\n" +
				"                    \"DOCK_NAME\":\"明东\",\n" +
				"                    \"WRAP_TYPE\":\"1\",\n" +
				"                    \"CNTR_NO\":\"TWCU8058021\",\n" +
				"                    \"SHIP_CODE\":\"UN7433177\",\n" +
				"                    \"VOYAGE\":\"1944W\",\n" +
				"                    \"REMARK\":\"\"\n" +
				"                }\n" +
				"            },\n" +
				"            {\n" +
				"                \"DockInfo\":{\n" +
				"                    \"SHIP_NAME_CN\":\"秋锦11\",\n" +
				"                    \"TRAF_MODE\":\"21\",\n" +
				"                    \"NODE_3\":\"NODE_3\",\n" +
				"                    \"NODE_2\":\"NODE_2\",\n" +
				"                    \"OPER_TIME\":\"20191108152711\",\n" +
				"                    \"SHIP_NAME_EN\":\"QIU JIN11\",\n" +
				"                    \"NODE_1\":\"NODE_1\",\n" +
				"                    \"STATUS_CODE\":\"10611\",\n" +
				"                    \"AREA_CODE\":\"310001\",\n" +
				"                    \"CNTR_TYPE\":\"H111\",\n" +
				"                    \"BL_NO\":\"JJHOSSHWNC9Y111\",\n" +
				"                    \"DOCK_NAME\":\"明1\",\n" +
				"                    \"WRAP_TYPE\":\"11\",\n" +
				"                    \"CNTR_NO\":\"TWCU805802111\",\n" +
				"                    \"SHIP_CODE\":\"UN8905232\",\n" +
				"                    \"VOYAGE\":\"1911W\",\n" +
				"                    \"REMARK\":\"11\"\n" +
				"                }\n" +
				"            }\n" +
				"        ]\n" +
				"    },\n" +
				"    \"ExtraInfo\":{\n" +
				"        \"receiver\":\"edi\",\n" +
				"        \"sender\":\"1\"\n" +
				"    },\n" +
				"    \"EnvelopInfo\":{\n" +
				"        \"messageType\":\"ECL300\",\n" +
				"        \"messageId\":\"ECL310000290020191111164231018193000\",\n" +
				"        \"version\":\"1.0\",\n" +
				"        \"sendTime\":\"20191111164231\"\n" +
				"    }\n" +
				"}";
		
		String msg1 = "";
		
		tEnv.executeSql("" +
				"insert into kafka_source_data(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
				"select '1359023762372042843' as msgId," +
				"'ECL300' as bizId," +
				"'message_data' as msgType," +
				"'null' as bizUniqueId," +
				"'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
				"'" + msg + "'" + "as parseData");
	}
}
