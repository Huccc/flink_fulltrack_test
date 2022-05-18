package com.chao.flinksql.cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class cuschk_test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);

        tEnv.executeSql("" +
                "CREATE TABLE KAFKA_DATA_XPQ_DB_PARSE_RESULT (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING,\n" +
                "  msgType STRING,\n" +
                "  bizUniqueId STRING,\n" +
                "  destination STRING,\n" +
                "  parseData STRING,\n" +
                "  `proctime` AS PROCTIME() + INTERVAL '8' HOURS\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'data-xpq-db-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ")\n");

//        tEnv.executeSql("" +
//                "insert into KAFKA_DATA_XPQ_DB_PARSE_RESULT(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
//                "select '1359023762372042845' as msgId," +
//                "'ogg_data' as bizId," +
//                "'null' as msgType," +
//                "'null' as bizUniqueId," +
//                "'SRC_XIB3.EDI_CUSCHK_BILLINFO' as destination," +
//                "'{\"ID\":98316,\"VESSELNAME_CN\":\"亿通测试\"}' as parseData");

        tEnv.executeSql("" +
                "insert into KAFKA_DATA_XPQ_DB_PARSE_RESULT(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
                "select '1359023762372042843' as msgId," +
                "'ogg_data' as bizId," +
                "'null' as msgType," +
                "'cuschk_test_ctnr' as bizUniqueId," +
//                "'cuschk_test_bill' as bizUniqueId," +
                "'SRC_XIB3.EDI_CUSCHK_CTNINFO' as destination," +
//                "'SRC_XIB3.EDI_CUSCHK_BILLINFO' as destination," +
                "'{\"ID\":98308,\"MSGLOGID\":\"31111\",\"MSGTYPE\":null,\"ENTRYID\":null,\"EXAMRECID\":null,\"EXAMMODE\":null,\"IEFLAG\":\"E\",\"VSLNAME\":\"COSCO MALAYSIA\",\"VOYAGE\":\"ac7\",\"BLNO\":\"100002\",\"TRADENAME\":null,\"OWNERNAME\":null,\"AGENTNAME\":null,\"DISCHARGE_PLACE\":null,\"CUSTOMS_DISTRICT\":null,\"TIMEFLAG\":\"2022-04-07 00:01:25\",\"FREEFLAG\":null,\"OP_TYPE\":\"I\",\"F_TAG\":null,\"MATCH_FLAG\":null,\"MSG2DB_TIME\":null,\"CAPXTIMESTAMP\":null,\"CHECKID\":null,\"PARENTLOGID\":\"31111\",\"CTNNO\":\"100003\",\"CAPXTIMESTAMP\":null,\"CHECKTYPE\":null}' as parseData");
    }
}
