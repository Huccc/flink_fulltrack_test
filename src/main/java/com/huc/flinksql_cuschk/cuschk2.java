package com.huc.flinksql_cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class cuschk2 {
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
                "  parseData STRING, -- 此处是 JSON 字符串，需要使用 UDF 进行转换\n" +
                "  `proctime` AS PROCTIME() + INTERVAL '8' HOURS\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'data-xpq-db-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
//                "  'scan.startup.mode' = 'earliest-offset'\n" +
                ")\n");

//        tEnv.executeSql("select msgId,bizId from KAFKA_DATA_XPQ_DB_PARSE_RESULT").print();

        tEnv.executeSql("" +
                "CREATE TABLE REDIS_DIM (\n" +
                "  key STRING,\n" +
                "  field STRING,\n" +
                "  `value` STRING\n" +
                ") WITH (\n" +
                "  'connector.type' = 'redis',\n" +
                "  'redis.ip' = '192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379,192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379',\n" +
                "  'database.num' = '0',\n" +
                "  'operate.type' = 'hash',\n" +
                "  'redis.version' = '2.6'\n" +
                ")\n");

        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_SUBSCRIBE_PARAM (\n" +
                "    APP_NAME STRING,\n" +
                "    TABLE_NAME STRING,\n" +
                "    SUBSCRIBE_TYPE STRING,\n" +
                "    SUBSCRIBER STRING,\n" +
                "    DB_URL STRING,\n" +
                "    KAFKA_SERVERS STRING,\n" +
                "    KAFKA_SINK_TOPIC STRING,\n" +
                "    ISCURRENT DECIMAL(10, 0), -- 如果需要 LEFT JOIN 'TEMPRAL TABLE JOIN' && WHERE ISCURRENT=XXX，必须要 >= 10，否则会报 scala.MatchError: (CAST(CAST($7):DECIMAL(6, 0)):DECIMAL(10, 0),1:DECIMAL(10, 0)) (of class scala.Tuple2)\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    PRIMARY KEY(APP_NAME, TABLE_NAME) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'adm_bdpp',\n" +
                "    'password' = 'easipass',\n" +
                "    'lookup.cache.max-rows' = '200',\n" +
                "    'lookup.cache.ttl' = '60', -- 单位：s\n" +
                "    'lookup.max-retries' = '3'\n" +
                ")\n");

//        tEnv.executeSql("select * from ORACLE_SUBSCRIBE_PARAM").print();

        // TODO oracle提单状态表 加主键
        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_TRACK_BIZ_STATUS_BILL(\n" +
                "    BL_NO STRING,\n" +
                "    MASTER_BL_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED DECIMAL(1, 0), -- Oracle NUMBER(1, 0) <-> Flink DECIMAL(1, 0)\n" +
                "    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, BL_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'dm',\n" +
                "    'password' = 'easipass'\n" +
                ")\n");

//        tEnv.executeSql("select * from ORACLE_TRACK_BIZ_STATUS_BILL").print();

        // TODO oracle提单表 维表
        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_BILL_DIM(\n" +
                "    BL_NO STRING,\n" +
                "    MASTER_BL_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED DECIMAL(1, 0), -- Oracle NUMBER(1, 0) <-> Flink DECIMAL(1, 0)\n" +
                "    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, BL_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'dm',\n" +
                "    'password' = 'easipass'\n" +
                ")\n");

//        tEnv.executeSql("select * from ORACLE_BILL_DIM").print();

        tEnv.executeSql("" +
                "CREATE TABLE KAFKA_TRACK_BIZ_STATUS_BILL (\n" +
                "  GID STRING,\n" +
                "  APP_NAME STRING,\n" +
                "  TABLE_NAME STRING,\n" +
                "  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送\n" +
                "  DATA ROW(\n" +
                "    BL_NO STRING,\n" +
                "    MASTER_BL_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED INT -- 对应于 JSON number 类型\n" +
                "  )\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'data-bdp-push',\n" +
                " 'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                " 'properties.group.id' = 'data-bdp-push',\n" +
                " 'format' = 'json'\n" +
                ")\n");

//        tEnv.executeSql("select * from KAFKA_TRACK_BIZ_STATUS_BILL").print();

        // TODO oracle箱状态表 加主键
        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_TRACK_BIZ_STATUS_CTNR (\n" +
                "    CTNR_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED DECIMAL(1),\n" +
                "    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, CTNR_NO, BIZ_STATUS_CODE) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'dm',\n" +
                "    'password' = 'easipass'\n" +
                ")\n");

//        tEnv.executeSql("select * from ORACLE_TRACK_BIZ_STATUS_CTNR").print();

        // TODO oracle箱表 维表
        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_CTNR_DIM (\n" +
                "    CTNR_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED DECIMAL(1),\n" +
                "    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, CTNR_NO, BIZ_STATUS_CODE) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'dm',\n" +
                "    'password' = 'easipass'\n" +
                ")\n");

        tEnv.executeSql("" +
                "CREATE TABLE KAFKA_TRACK_BIZ_STATUS_CTNR (\n" +
                "  GID STRING,\n" +
                "  APP_NAME STRING,\n" +
                "  TABLE_NAME STRING,\n" +
                "  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送\n" +
                "  DATA ROW(\n" +
                "    CTNR_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED INT -- 对应 JSON number 类型\n" +
                "  )\n" +
                ") WITH (\n" +
                " 'connector' = 'kafka',\n" +
                " 'topic' = 'data-bdp-push',\n" +
                " 'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                " 'properties.group.id' = 'data-bdp-push',\n" +
                " 'format' = 'json'\n" +
                ")\n");

//        tEnv.executeSql("select * from KAFKA_TRACK_BIZ_STATUS_CTNR").print();

        tEnv.executeSql("" +
                "CREATE VIEW TMP_BILL_INFO (\n" +
                "  GID, -- Global ID\n" +
                "  `map`,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  msgId AS GID,\n" +
                "  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS `map`, --JSON_TO_MAP(parseData)\n" +
                "  `proctime`\n" +
                "FROM KAFKA_DATA_XPQ_DB_PARSE_RESULT\n" +
                "WHERE bizId = 'ogg_data' AND destination = 'SRC_XIB3.EDI_CUSCHK_BILLINFO'\n");

//        tEnv.executeSql("select * from TMP_BILL_INFO").print();

//        Table table = tEnv.sqlQuery("select * from TMP_BILL_INFO");
//        tEnv.toAppendStream(table, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "CREATE VIEW BILL_INFO(\n" +
                "  GID, -- \"提单\" 的 Global ID，不同于 “箱”\n" +
                "  MSGLOGID, -- 用于和 “箱” 关联\n" +
                "  MSGTYPE,\n" +
                "  ENTRYID,\n" +
                "  EXAMRECID,\n" +
                "  EXAMMODE,\n" +
                "  IEFLAG,\n" +
                "  VSLNAME,\n" +
                "  VOYAGE,\n" +
                "  BLNO,\n" +
                "  TRADENAME,\n" +
                "  OWNERNAME,\n" +
                "  AGENTNAME,\n" +
                "  DISCHARGE_PLACE,\n" +
                "  CUSTOMS_DISTRICT,\n" +
                "  TIMEFLAG,\n" +
                "  FREEFLAG,\n" +
                "  F_TAG,\n" +
                "  MATCH_FLAG,\n" +
                "  MSG2DB_TIME,\n" +
                "  CAPXTIMESTAMP,\n" +
                "  CHECKID,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  GID,\n" +
                "  `map`['MSGLOGID'],\n" +
                "  `map`['MSGTYPE'],\n" +
                "  `map`['ENTRYID'],\n" +
                "  `map`['EXAMRECID'],\n" +
                "  `map`['EXAMMODE'],\n" +
                "  `map`['IEFLAG'],\n" +
                "  `map`['VSLNAME'],\n" +
                "  `map`['VOYAGE'],\n" +
                "  `map`['BLNO'],\n" +
                "  `map`['TRADENAME'],\n" +
                "  `map`['OWNERNAME'],\n" +
                "  `map`['AGENTNAME'],\n" +
                "  `map`['DISCHARGE_PLACE'],\n" +
                "  `map`['CUSTOMS_DISTRICT'],\n" +
                "  `map`['TIMEFLAG'],\n" +
                "  `map`['FREEFLAG'],\n" +
                "  `map`['F_TAG'],\n" +
                "  `map`['MATCH_FLAG'],\n" +
                "  `map`['MSG2DB_TIME'],\n" +
                "  `map`['CAPXTIMESTAMP'],\n" +
                "  `map`['CHECKID'],\n" +
                "  `proctime`\n" +
                "FROM TMP_BILL_INFO\n" +
                "WHERE `map`['OP_TYPE'] = 'I'\n");

//        Table table = tEnv.sqlQuery("select * from BILL_INFO");
//        tEnv.toAppendStream(table, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "CREATE VIEW TMP_CTNR_INFO (\n" +
                "  GID, -- Global ID\n" +
                "  `map`,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  msgId AS GID,\n" +
                "  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS `map`, --JSON_TO_MAP(parseData)\n" +
                "  `proctime`\n" +
                "FROM KAFKA_DATA_XPQ_DB_PARSE_RESULT\n" +
                "WHERE bizId = 'ogg_data' AND destination = 'SRC_XIB3.EDI_CUSCHK_CTNINFO'\n");

//        Table table = tEnv.sqlQuery("select * from TMP_CTNR_INFO");
//        tEnv.toAppendStream(table, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "CREATE VIEW CTNR_INFO (\n" +
                "  GID, -- \"箱\" 的 Global ID，不同于 “提单”\n" +
                "  ID,\n" +
                "  PARENTLOGID, -- 用于和 “提单” 关联\n" +
                "  CTNNO,\n" +
                "  CAPXTIMESTAMP,\n" +
                "  CHECKTYPE,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  GID,\n" +
                "  `map`['ID'],\n" +
                "  `map`['PARENTLOGID'],\n" +
                "  `map`['CTNNO'],\n" +
                "  `map`['CAPXTIMESTAMP'],\n" +
                "  `map`['CHECKTYPE'],\n" +
                "  `proctime`\n" +
                "FROM TMP_CTNR_INFO\n");

        Table table3 = tEnv.sqlQuery("select * from CTNR_INFO");
        tEnv.toAppendStream(table3, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "CREATE VIEW BILL (\n" +
                "  GID, -- “提单” Global ID\n" +
                "  MSGLOGID, -- 用于和 “箱” 关联\n" +
                "  BL_NO,\n" +
                "  MASTER_BL_NO,\n" +
                "  VSL_IMO_NO,\n" +
                "  VSL_NAME,\n" +
                "  VOYAGE,\n" +
                "  ACCURATE_IMONO,\n" +
                "  ACCURATE_VSLNAME,\n" +
                "  I_E_MARK,\n" +
                "  BIZ_STAGE_NO,\n" +
                "  BIZ_STAGE_CODE,\n" +
                "  BIZ_STAGE_NAME,\n" +
                "  BIZ_TIME,\n" +
                "  BIZ_STATUS_CODE,\n" +
                "  BIZ_STATUS,\n" +
                "  BIZ_STATUS_DESC,\n" +
                "  LASTUPDATEDDT,\n" +
                "  ISDELETED,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  GID,\n" +
                "  MSGLOGID,\n" +
                "  IF(BLNO <> '', BLNO, 'N/A') AS BL_NO,\n" +
                "  'N/A' AS MASTER_BL_NO,\n" +
                "  IF(dim_ship1.`value` <> '', dim_ship1.`value`, 'N/A') AS VSL_IMO_NO, -- 同 ACCURATE_IMONO\n" +
                "  IF(VSLNAME <> '', UPPER(TRIM(REGEXP_REPLACE(VSLNAME, '[\\t\\n\\r]', ''))), 'N/A') AS VSL_NAME,\n" +
                "  IF(VOYAGE <> '', UPPER(TRIM(REGEXP_REPLACE(VOYAGE, '[\\t\\n\\r]', ''))), 'N/A') AS VOYAGE,\n" +
                "  IF(dim_ship1.`value` <> '', dim_ship1.`value`, 'N/A') AS ACCURATE_IMONO,\n" +
                "  IF(dim_ship2.`value` <> '', dim_ship2.`value`, 'N/A') AS ACCURATE_VSLNAME,\n" +
                "  IF(IEFLAG <> '', IEFLAG, 'N/A') AS I_E_MARK,\n" +
                "  CASE IEFLAG WHEN 'E' THEN 'C7.6' WHEN 'I' THEN 'D6.10' ELSE 'N/A' END AS BIZ_STAGE_NO,\n" +
                "  CASE IEFLAG WHEN 'E' THEN 'E_cusDecl_chk' WHEN 'I' THEN 'I_cusDecl_chk' ELSE 'N/A' END AS BIZ_STAGE_CODE,\n" +
                "  IF(dim_biz_stage.`value` <> '', dim_biz_stage.`value`, 'N/A') AS BIZ_STAGE_NAME,\n" +
                "  TO_TIMESTAMP(TIMEFLAG, 'yyyy-MM-dd HH:mm:ss') AS BIZ_TIME,\n" +
                "  IF(FREEFLAG <> '', FREEFLAG, 'C') AS BIZ_STATUS_CODE,\n" +
                "  IF(dim_common_mini.`value` <> '', dim_common_mini.`value`, 'N/A') AS BIZ_STATUS,\n" +
                "  '' AS BIZ_STATUS_DESC,\n" +
                "  CAST(`proctime` AS TIMESTAMP(3)) AS LASTUPDATEDDT,\n" +
                "  CAST(0 AS DECIMAL(1)) AS ISDELETED, -- Flink INT -> Flink DECIMAL，类型映射关系： Oracle NUMBER <-> Flink DECIMAL\n" +
                "  `proctime`\n" +
                "FROM BILL_INFO AS BI\n" +
                "  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_ship1\n" +
                "    ON dim_ship1.key = CONCAT('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=', UPPER(TRIM(REGEXP_REPLACE(BI.VSLNAME, '[\\t\\n\\r]', '')))) AND dim_ship1.field = 'IMO_NO'\n" +
                "  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_ship2\n" +
                "    ON dim_ship2.key = CONCAT('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=', UPPER(TRIM(REGEXP_REPLACE(BI.VSLNAME, '[\\t\\n\\r]', '')))) AND dim_ship2.field = 'VSL_NAME_EN'\n" +
                "  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_biz_stage\n" +
                "    ON dim_biz_stage.key = CONCAT('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',\n" +
                "        CASE IEFLAG WHEN 'E' THEN 'C7.6' WHEN 'I' THEN 'D6.10' ELSE 'N/A' END,\n" +
                "        '&SUB_STAGE_CODE=',\n" +
                "        CASE IEFLAG WHEN 'E' THEN 'E_cusDecl_chk' WHEN 'I' THEN 'I_cusDecl_chk' ELSE 'N/A' END\n" +
                "    ) AND dim_biz_stage.field = 'SUB_STAGE_NAME'\n" +
                "  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_common_mini\n" +
                "    ON dim_common_mini.key = CONCAT('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=cus_check_result&TYPE_CODE=', IF(BI.FREEFLAG <> '' , BI.FREEFLAG, 'C'))\n" +
                "    AND dim_common_mini.field = 'TYPE_NAME'\n");

        Table table = tEnv.sqlQuery("select * from BILL");
        tEnv.toAppendStream(table, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "CREATE VIEW CTNR (\n" +
                "  GID, -- “箱” 的 Global ID，不同于 “提单”\n" +
                "  CTNR_NO,\n" +
                "  VSL_IMO_NO,\n" +
                "  VSL_NAME,\n" +
                "  VOYAGE,\n" +
                "  ACCURATE_IMONO,\n" +
                "  ACCURATE_VSLNAME,\n" +
                "  I_E_MARK,\n" +
                "  BIZ_STAGE_NO,\n" +
                "  BIZ_STAGE_CODE,\n" +
                "  BIZ_STAGE_NAME,\n" +
                "  BIZ_TIME,\n" +
                "  BIZ_STATUS_CODE,\n" +
                "  BIZ_STATUS,\n" +
                "  BIZ_STATUS_DESC,\n" +
                "  LASTUPDATEDDT,\n" +
                "  ISDELETED,\n" +
                "  `proctime`\n" +
                ") AS\n" +
                "SELECT\n" +
                "  CI.GID,\n" +
                "  CI.CTNNO,\n" +
                "  IF(B.VSL_IMO_NO <> '', B.VSL_IMO_NO, 'N/A'),\n" +
                "  IF(B.VSL_NAME <> '', B.VSL_NAME, 'N/A'),\n" +
                "  IF(B.VOYAGE <> '', B.VOYAGE, 'N/A'),\n" +
                "  IF(B.ACCURATE_IMONO <> '', B.ACCURATE_IMONO, 'N/A'),\n" +
                "  IF(B.ACCURATE_VSLNAME <> '', B.ACCURATE_VSLNAME, 'N/A'),\n" +
                "  IF(B.I_E_MARK <> '', B.I_E_MARK, 'N/A'),\n" +
                "  IF(B.BIZ_STAGE_NO <> '', B.BIZ_STAGE_NO, 'N/A'),\n" +
                "  IF(B.BIZ_STAGE_CODE <> '', B.BIZ_STAGE_CODE, 'N/A'),\n" +
                "  IF(B.BIZ_STAGE_NAME <> '', B.BIZ_STAGE_NAME, 'N/A'),\n" +
                "  CAST(B.BIZ_TIME AS TIMESTAMP(3)),\n" +
                "  IF(B.BIZ_STATUS_CODE <> '', B.BIZ_STATUS_CODE, 'N/A'),\n" +
                "  IF(B.BIZ_STATUS <> '', B.BIZ_STATUS, 'N/A'),\n" +
                "  B.BIZ_STATUS_DESC, -- 不做判断，默认值用 ''\n" +
                "  CAST(CI.`proctime` AS TIMESTAMP(3)) AS LASTUPDATEDDT, -- PROCTIME() 无法识别时区\n" +
                "  B.ISDELETED,\n" +
                "  CI.`proctime` AS `proctime`\n" +
                "FROM CTNR_INFO AS CI\n" +
                "  JOIN BILL AS B ON CI.PARENTLOGID = B.MSGLOGID\n");

        Table table1 = tEnv.sqlQuery("select * from CTNR");
        tEnv.toAppendStream(table1, Row.class).print();
        env.execute();

    }
}
