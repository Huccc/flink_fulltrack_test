package com.huc.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class cuschk_test1 {
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
                "  'topic' = 'data-xpq-db-parse-result2',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ")\n");

        tEnv.executeSql("" +
                "--船舶状态表\n" +
                "insert into KAFKA_DATA_XPQ_DB_PARSE_RESULT(msgId,bizId,msgType,bizUniqueId,destination,parseData) " +
                "select '1359023762372042843' as msgId," +
                "'ogg_data' as bizId," +
                "'null' as msgType," +
                "'null' as bizUniqueId," +
                "'SRC_XIB3.EDI_CUSCHK_BILLINFO' as destination," +
                "'{\"ID\":\"101\",\"MSGLOGID\":\"11111\",\"MSGTYPE\":\"\",\"ENTRYID\":\"\",\"EXAMRECID\":\"\",\"EXAMMODE\":\"\",\"IEFLAG\":\"E\",\"VSLNAME\":\"COSCO MALAYSIA\",\"VOYAGE\":\"E7\",\"BLNO\":\"10001\",\"TRADENAME\":\"\",\"OWNERNAME\":\"\",\"AGENTNAME\":\"\",\"DISCHARGE_PLACE\":\"\",\"CUSTOMS_DISTRICT\":\"\",\"TIMEFLAG\":\"2022-04-07 00:00:00\",\"FREEFLAG\":\"2\",\"F_TAG\":\"\",\"MATCH_FLAG\":\"\",\"MSG2DB_TIME\":\"\",\"CAPXTIMESTAMP\":\"\",\"CHECKID\":\"\",\"PARENTLOGID\":\"11111\",\"CTNNO\":\"10002\",\"CAPXTIMESTAMP\":\"\",\"CHECKTYPE\":\"\"}' as parseData");

    }
}
