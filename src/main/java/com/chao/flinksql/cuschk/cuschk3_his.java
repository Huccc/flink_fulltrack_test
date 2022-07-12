package com.chao.flinksql.cuschk;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class cuschk3_his {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE KAFKA_DATA_XPQ_DB_PARSE_RESULT (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING, -- 此处是 JSON 字符串，需要使用 UDF 进行转换\n" +
				"  `proctime` AS PROCTIME() + INTERVAL '8' HOURS,\n" +
				"  wintime AS PROCTIME()\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'data-xpq-db-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-cuschk',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
//		Table KAFKA_DATA_XPQ_DB_PARSE_RESULT = tEnv.sqlQuery("select * from KAFKA_DATA_XPQ_DB_PARSE_RESULT");
//		tEnv.toAppendStream(KAFKA_DATA_XPQ_DB_PARSE_RESULT,Row.class).print();
//		env.execute();
		
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
				")");
		
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
				"    'lookup.cache.ttl' = '60', -- 单位 s\n" +
				"    'lookup.max-retries' = '3'\n" +
				")");

		// TODO Oracle箱单关系表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_blctnr_dim (\n" +
				"    VSL_IMO_NO STRING,\n" +
				"    VSL_NAME STRING,\n" +
				"    VOYAGE STRING,\n" +
				"    ACCURATE_IMONO STRING,\n" +
				"    ACCURATE_VSLNAME STRING,\n" +
				"    BL_NO STRING,\n" +
				"    MASTER_BL_NO STRING,\n" +
				"    CTNR_NO STRING,\n" +
				"    I_E_MARK STRING,\n" +
				"    MESSAGE_ID STRING,\n" +
				"    SENDER_CODE STRING,\n" +
				"    BULK_FLAG STRING,\n" +
				"    RSP_CREATE_TIME TIMESTAMP,\n" +
				"    LASTUPDATEDDT TIMESTAMP,\n" +
				"    ISDELETED DECIMAL(22, 0),\n" +
				"    PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,CTNR_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_BLCTNR',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_TRACK_BIZ_STATUS_BILL( \n" +
				"                    UUID STRING, \n" +
				"                    BL_NO STRING, \n" +
				"                    MASTER_BL_NO STRING, \n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),  \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED DECIMAL(1, 0), -- Oracle NUMBER(1, 0) <-> Flink DECIMAL(1, 0) \n" +
				"                    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, BL_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉 \n" +
				"                ) WITH ( \n" +
				"                    'connector' = 'jdbc', \n" +
				"                    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c', \n" +
				"                    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL', \n" +
				"                    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver \n" +
				"                    'username' = 'dm', \n" +
				"                    'password' = 'easipass' \n" +
				"                )");
		
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_BILL_DIM( \n" +
				"                    UUID STRING, \n" +
				"                    BL_NO STRING, \n" +
				"                    MASTER_BL_NO STRING, \n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),  \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED DECIMAL(1, 0), -- Oracle NUMBER(1, 0) <-> Flink DECIMAL(1, 0) \n" +
				"                    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, BL_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉 \n" +
				"                ) WITH ( \n" +
				"                    'connector' = 'jdbc', \n" +
				"                    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c', \n" +
				"                    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL', \n" +
				"                    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver \n" +
				"                    'username' = 'dm', \n" +
				"                    'password' = 'easipass' \n" +
				"                )");
		
		tEnv.executeSql("" +
				"CREATE TABLE KAFKA_TRACK_BIZ_STATUS_BILL ( \n" +
				"                  GID STRING, \n" +
				"                  APP_NAME STRING, \n" +
				"                  TABLE_NAME STRING, \n" +
				"                  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送 \n" +
				"                  DATA ROW(\n" +
				"                  UUID STRING,\n" +
				"                    BL_NO STRING,\n" +
				"                    MASTER_BL_NO STRING,\n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE int, \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED INT -- 对应于 JSON number 类型 \n" +
				"                  ) \n" +
				"                ) WITH ( \n" +
				"                 'connector' = 'kafka', \n" +
				"                 'topic' = 'data-bdp-push', \n" +
				"                 'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092', \n" +
				"                 'properties.group.id' = 'data-bdp-push', \n" +
				"                 'format' = 'json' \n" +
				"                )");
		
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_TRACK_BIZ_STATUS_CTNR (\n" +
				"                    UUID STRING, \n" +
				"                    CTNR_NO STRING, \n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),  \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED DECIMAL(1), \n" +
				"                    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, CTNR_NO, BIZ_STATUS_CODE) NOT ENFORCED \n" +
				"                ) WITH ( \n" +
				"                    'connector' = 'jdbc', \n" +
				"                    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c', \n" +
				"                    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR', \n" +
				"                    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver \n" +
				"                    'username' = 'dm', \n" +
				"                    'password' = 'easipass' \n" +
				"                )");
		
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_CTNR_DIM ( \n" +
				"                    UUID STRING, \n" +
				"                    CTNR_NO STRING, \n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),  \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED DECIMAL(1), \n" +
				"                    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, CTNR_NO, BIZ_STATUS_CODE) NOT ENFORCED \n" +
				"                ) WITH ( \n" +
				"                    'connector' = 'jdbc', \n" +
				"                    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c', \n" +
				"                    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR', \n" +
				"                    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver \n" +
				"                    'username' = 'dm', \n" +
				"                    'password' = 'easipass' \n" +
				"                )");
		
		
		tEnv.executeSql("" +
				"CREATE TABLE KAFKA_TRACK_BIZ_STATUS_CTNR ( \n" +
				"                  GID STRING, \n" +
				"                  APP_NAME STRING, \n" +
				"                  TABLE_NAME STRING, \n" +
				"                  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送 \n" +
				"                  DATA ROW(\n" +
				"                  UUID STRING,\n" +
				"                    CTNR_NO STRING, \n" +
				"                    VSL_IMO_NO STRING, \n" +
				"                    VSL_NAME STRING, \n" +
				"                    VOYAGE STRING, \n" +
				"                    ACCURATE_IMONO STRING, \n" +
				"                    ACCURATE_VSLNAME STRING, \n" +
				"                    I_E_MARK STRING, \n" +
				"                    BIZ_STAGE_NO STRING, \n" +
				"                    BIZ_STAGE_CODE STRING, \n" +
				"                    BIZ_STAGE_NAME STRING, \n" +
				"                    BIZ_TIME TIMESTAMP(3), \n" +
				"                    BIZ_STATUS_CODE STRING, \n" +
				"                    BIZ_STATUS STRING, \n" +
				"                    BIZ_STATUS_IFFECTIVE int, \n" +
				"                    BIZ_STATUS_DESC STRING, \n" +
				"                    LASTUPDATEDDT TIMESTAMP(3), \n" +
				"                    ISDELETED INT -- 对应 JSON number 类型 \n" +
				"                  ) \n" +
				"                ) WITH ( \n" +
				"                 'connector' = 'kafka', \n" +
				"                 'topic' = 'data-bdp-push', \n" +
				"                 'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092', \n" +
				"                 'properties.group.id' = 'data-bdp-push', \n" +
				"                 'format' = 'json' \n" +
				"                )");
		
		tEnv.executeSql("" +
				"CREATE VIEW TMP_BILL_INFO ( \n" +
				"                  `map`, \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS `map`, --JSON_TO_MAP(parseData) \n" +
				"                  `proctime`,concat(msgId, '^', bizUniqueId, '^', bizId) as GID ,wintime\n" +
				"                FROM KAFKA_DATA_XPQ_DB_PARSE_RESULT \n" +
				"                WHERE bizId = 'ogg_data' AND destination = 'SRC_XIB3.EDI_CUSCHK_BILLINFO'");
		
		tEnv.executeSql("" +
				"CREATE VIEW BILL_INFO( \n" +
				"                  MSGLOGID, -- 用于和 “箱” 关联 \n" +
				"                  MSGTYPE, \n" +
				"                  ENTRYID, \n" +
				"                  EXAMRECID, \n" +
				"                  EXAMMODE, \n" +
				"                  IEFLAG, \n" +
				"                  VSLNAME, \n" +
				"                  VOYAGE, \n" +
				"                  BLNO, \n" +
				"                  TRADENAME, \n" +
				"                  OWNERNAME, \n" +
				"                  AGENTNAME, \n" +
				"                  DISCHARGE_PLACE, \n" +
				"                  CUSTOMS_DISTRICT, \n" +
				"                  TIMEFLAG, \n" +
				"                  FREEFLAG, \n" +
				"                  F_TAG, \n" +
				"                  MATCH_FLAG, \n" +
				"                  MSG2DB_TIME, \n" +
				"                  CAPXTIMESTAMP, \n" +
				"                  CHECKID, \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  `map`['MSGLOGID'], \n" +
				"                  `map`['MSGTYPE'], \n" +
				"                  `map`['ENTRYID'], \n" +
				"                  `map`['EXAMRECID'], \n" +
				"                  `map`['EXAMMODE'], \n" +
				"                  `map`['IEFLAG'], \n" +
				"                  `map`['VSLNAME'], \n" +
				"                  `map`['VOYAGE'], \n" +
				"                  `map`['BLNO'], \n" +
				"                  `map`['TRADENAME'], \n" +
				"                  `map`['OWNERNAME'], \n" +
				"                  `map`['AGENTNAME'], \n" +
				"                  `map`['DISCHARGE_PLACE'], \n" +
				"                  `map`['CUSTOMS_DISTRICT'], \n" +
				"                  `map`['TIMEFLAG'], \n" +
				"                  `map`['FREEFLAG'], \n" +
				"                  `map`['F_TAG'], \n" +
				"                  `map`['MATCH_FLAG'], \n" +
				"                  `map`['MSG2DB_TIME'], \n" +
				"                  `map`['CAPXTIMESTAMP'], \n" +
				"                  `map`['CHECKID'], \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                FROM TMP_BILL_INFO \n" +
				"                WHERE `map`['OP_TYPE'] = 'I'");
		
		tEnv.executeSql("" +
				"CREATE VIEW TMP_CTNR_INFO ( \n" +
				"                  `map`, \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS `map`, \n" +
				"                  `proctime` ,concat(msgId, '^', bizUniqueId, '^', bizId) as GID ,wintime\n" +
				"                FROM KAFKA_DATA_XPQ_DB_PARSE_RESULT \n" +
				"                WHERE bizId = 'ogg_data' AND destination = 'SRC_XIB3.EDI_CUSCHK_CTNINFO'");
		
		tEnv.executeSql("" +
				"CREATE VIEW CTNR_INFO ( \n" +
				"                  ID, \n" +
				"                  PARENTLOGID, -- 用于和 “提单” 关联 \n" +
				"                  CTNNO, \n" +
				"                  CAPXTIMESTAMP, \n" +
				"                  CHECKTYPE, \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  `map`['ID'], \n" +
				"                  `map`['PARENTLOGID'], \n" +
				"                  `map`['CTNNO'], \n" +
				"                  `map`['CAPXTIMESTAMP'], \n" +
				"                  `map`['CHECKTYPE'], \n" +
				"                  `proctime` ,GID ,wintime\n" +
				"                FROM TMP_CTNR_INFO");
		
		tEnv.executeSql("" +
				"CREATE VIEW RESBILL_TMP_ALL (\n" +
				"                  UUID, GID, wintime,\n" +
				"                  MSGLOGID, -- 用于和 “箱” 关联 \n" +
				"                  BL_NO, \n" +
				"                  MASTER_BL_NO, \n" +
				"                  VSL_IMO_NO, \n" +
				"                  VSL_NAME, \n" +
				"                  VOYAGE, \n" +
				"                  ACCURATE_IMONO, \n" +
				"                  ACCURATE_VSLNAME, \n" +
				"                  I_E_MARK, \n" +
				"                  BIZ_STAGE_NO, \n" +
				"                  BIZ_STAGE_CODE, \n" +
				"                  BIZ_STAGE_NAME, \n" +
				"                  BIZ_TIME, \n" +
				"                  BIZ_STATUS_CODE, \n" +
				"                  BIZ_STATUS, \n" +
				"                  BIZ_STATUS_IFFECTIVE, \n" +
				"                  BIZ_STATUS_DESC, \n" +
				"                  LASTUPDATEDDT, \n" +
				"                  ISDELETED, \n" +
				"                  `proctime` \n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  uuid() AS UUID, GID, wintime,\n" +
				"                  MSGLOGID,\n" +
				"                  IF(BLNO <> '', TRIM(REGEXP_REPLACE(BLNO, '[\\t\\n\\r]', '')), 'N/A') AS BL_NO,\n" +
				"                  'N/A' AS MASTER_BL_NO,\n" +
				"                  IF(obd.VSL_IMO_NO <> '', obd.VSL_IMO_NO, 'N/A') AS VSL_IMO_NO, -- 同 ACCURATE_IMONO\n" +
				"                  IF(VSLNAME <> '', UPPER(TRIM(REGEXP_REPLACE(VSLNAME, '[\\t\\n\\r]', ''))), 'N/A') AS VSL_NAME,\n" +
				"                  IF(BI.VOYAGE <> '', UPPER(TRIM(REGEXP_REPLACE(BI.VOYAGE, '[\\t\\n\\r]', ''))), 'N/A') AS VOYAGE,\n" +
				"                  IF(obd.VSL_IMO_NO <> '', obd.VSL_IMO_NO, 'N/A') AS ACCURATE_IMONO,\n" +
				"                  IF(dim_ship2.`value` <> '', dim_ship2.`value`, 'N/A') AS ACCURATE_VSLNAME, \n" +
				"                  IF(IEFLAG <> '', IEFLAG, 'N/A') AS I_E_MARK, \n" +
				"                  CASE IEFLAG WHEN 'E' THEN 'C7.6' WHEN 'I' THEN 'D6.10' ELSE 'N/A' END AS BIZ_STAGE_NO, \n" +
				"                  CASE IEFLAG WHEN 'E' THEN 'E_cusDecl_chk' WHEN 'I' THEN 'I_cusDecl_chk' ELSE 'N/A' END AS BIZ_STAGE_CODE, \n" +
				"                  IF(dim_biz_stage.`value` <> '', dim_biz_stage.`value`, 'N/A') AS BIZ_STAGE_NAME, \n" +
				"                  TO_TIMESTAMP(TIMEFLAG, 'yyyy-MM-dd HH:mm:ss') AS BIZ_TIME,\n" +
				"                  case FREEFLAG when '' then 'C' when 'null' then 'C' else FREEFLAG END AS BIZ_STATUS_CODE,\n" +
				"                  IF(dim_common_mini.`value` <> '', dim_common_mini.`value`, 'N/A') AS BIZ_STATUS,\n" +
				"                  1 AS BIZ_STATUS_IFFECTIVE, \n" +
				"                  '' AS BIZ_STATUS_DESC, \n" +
				"                  CAST(`proctime` AS TIMESTAMP(3)) AS LASTUPDATEDDT, \n" +
				"                  CAST(0 AS DECIMAL(1)) AS ISDELETED, -- Flink INT -> Flink DECIMAL，类型映射关系： Oracle NUMBER <-> Flink DECIMAL \n" +
				"                  `proctime` \n" +
				"                FROM BILL_INFO AS BI\n" +
				"                  LEFT JOIN oracle_blctnr_dim FOR SYSTEM_TIME AS OF BI.`proctime` AS obd\n" +
				"                    ON UPPER(TRIM(REGEXP_REPLACE(VSLNAME, '[\\t\\n\\r]', '')))=obd.VSL_NAME and UPPER(TRIM(REGEXP_REPLACE(BI.VOYAGE, '[\\t\\n\\r]', '')))=obd.VOYAGE and IF(IEFLAG <> '', IEFLAG, 'N/A')=obd.I_E_MARK and obd.ISDELETED=0\n" +
				"                  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_ship2 \n" +
				"                    ON dim_ship2.key = CONCAT('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=', UPPER(TRIM(REGEXP_REPLACE(BI.VSLNAME, '[\\t\\n\\r]', '')))) AND dim_ship2.field = 'VSL_NAME_EN'\n" +
				"                  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_biz_stage \n" +
				"                    ON dim_biz_stage.key = CONCAT('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=', \n" +
				"                        CASE IEFLAG WHEN 'E' THEN 'C7.6' WHEN 'I' THEN 'D6.10' ELSE 'N/A' END, \n" +
				"                        '&SUB_STAGE_CODE=', \n" +
				"                        CASE IEFLAG WHEN 'E' THEN 'E_cusDecl_chk' WHEN 'I' THEN 'I_cusDecl_chk' ELSE 'N/A' END \n" +
				"                    ) AND dim_biz_stage.field = 'SUB_STAGE_NAME' \n" +
				"                  LEFT JOIN REDIS_DIM FOR SYSTEM_TIME AS OF BI.`proctime` AS dim_common_mini \n" +
				"                    ON dim_common_mini.key = CONCAT('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=cus_check_result&TYPE_CODE=', case FREEFLAG when '' then 'C' when 'null' then 'C' else FREEFLAG END)\n" +
				"                    AND dim_common_mini.field = 'TYPE_NAME'");

		Table RESBILL_TMP_ALL = tEnv.sqlQuery("select * from RESBILL_TMP_ALL");
		tEnv.toAppendStream(RESBILL_TMP_ALL, Row.class).print();
//		env.execute();
		
		// TODO 获取业务主键的最大业务发生时间
		tEnv.executeSql("" +
				"create view res_tmp_maxtime as\n" +
				"select\n" +
				"  VSL_IMO_NO,\n" +
				"  VOYAGE,\n" +
				"  BL_NO,\n" +
				"  BIZ_STAGE_NO,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  max(BIZ_TIME) as BIZ_TIME\n" +
				"FROM RESBILL_TMP_ALL\n" +
				"GROUP BY\n" +
				"  TUMBLE(wintime, INTERVAL '2' minute),\n" +
				"  VSL_IMO_NO,\n" +
				"  VOYAGE,\n" +
				"  BL_NO,\n" +
				"  BIZ_STAGE_NO,\n" +
				"  BIZ_STATUS_CODE");
		
//		Table res_tmp_maxtime = tEnv.sqlQuery("select * from res_tmp_maxtime");
//		tEnv.toAppendStream(res_tmp_maxtime, Row.class).print();
//		env.execute();
		
		// TODO 获取业务主键最大业务发生时间的所有字段
		tEnv.executeSql("" +
				"create view BILL as\n" +
				"select RESBILL_TMP_ALL.*\n" +
				"from  RESBILL_TMP_ALL join res_tmp_maxtime\n" +
				"                           on RESBILL_TMP_ALL.VSL_IMO_NO=res_tmp_maxtime.VSL_IMO_NO\n" +
				"                           and RESBILL_TMP_ALL.VOYAGE=res_tmp_maxtime.VOYAGE\n" +
				"                           and RESBILL_TMP_ALL.BL_NO=res_tmp_maxtime.BL_NO\n" +
				"                           and RESBILL_TMP_ALL.BIZ_STAGE_NO=res_tmp_maxtime.BIZ_STAGE_NO\n" +
				"                           and RESBILL_TMP_ALL.BIZ_STATUS_CODE=res_tmp_maxtime.BIZ_STATUS_CODE\n" +
				"                           and RESBILL_TMP_ALL.BIZ_TIME=res_tmp_maxtime.BIZ_TIME");
		
		Table BILL = tEnv.sqlQuery("select * from BILL");
		tEnv.toAppendStream(BILL, Row.class).print();
		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		statementSet.addInsertSql("" +
				"INSERT INTO ORACLE_TRACK_BIZ_STATUS_BILL ( \n" +
				"                  UUID, \n" +
				"                  BL_NO, \n" +
				"                  MASTER_BL_NO, \n" +
				"                  VSL_IMO_NO, \n" +
				"                  VSL_NAME, \n" +
				"                  VOYAGE, \n" +
				"                  ACCURATE_IMONO, \n" +
				"                  ACCURATE_VSLNAME, \n" +
				"                  I_E_MARK, \n" +
				"                  BIZ_STAGE_NO, \n" +
				"                  BIZ_STAGE_CODE, \n" +
				"                  BIZ_STAGE_NAME, \n" +
				"                  BIZ_TIME, \n" +
				"                  BIZ_STATUS_CODE, \n" +
				"                  BIZ_STATUS, \n" +
				"                  BIZ_STATUS_IFFECTIVE, \n" +
				"                  BIZ_STATUS_DESC, \n" +
				"                  LASTUPDATEDDT, \n" +
				"                  ISDELETED \n" +
				"                ) \n" +
				"                SELECT \n" +
				"                  BILL.UUID, \n" +
				"                  BILL.BL_NO, \n" +
				"                  BILL.MASTER_BL_NO, \n" +
				"                  BILL.VSL_IMO_NO, \n" +
				"                  BILL.VSL_NAME, \n" +
				"                  BILL.VOYAGE, \n" +
				"                  BILL.ACCURATE_IMONO, \n" +
				"                  BILL.ACCURATE_VSLNAME, \n" +
				"                  BILL.I_E_MARK, \n" +
				"                  BILL.BIZ_STAGE_NO, \n" +
				"                  BILL.BIZ_STAGE_CODE, \n" +
				"                  BILL.BIZ_STAGE_NAME, \n" +
				"                  BILL.BIZ_TIME, \n" +
				"                  BILL.BIZ_STATUS_CODE, \n" +
				"                  BILL.BIZ_STATUS, \n" +
				"                  BILL.BIZ_STATUS_IFFECTIVE, \n" +
				"                  BILL.BIZ_STATUS_DESC, \n" +
				"                  BILL.LASTUPDATEDDT, \n" +
				"                  BILL.ISDELETED \n" +
				"                FROM BILL left join ORACLE_BILL_DIM FOR SYSTEM_TIME as OF BILL.`proctime` as obd \n" +
				"                 on BILL.VSL_IMO_NO=obd.VSL_IMO_NO \n" +
				"                 and BILL.VOYAGE=obd.VOYAGE \n" +
				"                 and BILL.BL_NO=obd.BL_NO \n" +
				"                 and BILL.BIZ_STAGE_NO=obd.BIZ_STAGE_NO \n" +
				"                 and BILL.BIZ_STATUS_CODE=obd.BIZ_STATUS_CODE \n" +
				"                where (obd.BIZ_TIME is null or BILL.BIZ_TIME>obd.BIZ_TIME) \n" +
				"                 and BILL.BIZ_TIME is not null");

//		statementSet.execute();
		
		statementSet.addInsertSql("" +
				"INSERT INTO KAFKA_TRACK_BIZ_STATUS_BILL ( \n" +
				"                  GID, \n" +
				"                  APP_NAME, \n" +
				"                  TABLE_NAME, \n" +
				"                  SUBSCRIBE_TYPE, \n" +
				"                  DATA \n" +
				"                ) \n" +
				"                SELECT \n" +
				"                  GID, \n" +
				"                  APP_NAME, \n" +
				"                  TABLE_NAME, \n" +
				"                  SUBSCRIBE_TYPE, \n" +
				"                  ROW(UUID, BL_NO, MASTER_BL_NO, VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, I_E_MARK, BIZ_STAGE_NO,\n" +
				"                    BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_IFFECTIVE, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED) AS DATA \n" +
				"                FROM ( \n" +
				"                  SELECT \n" +
				"                    b.GID AS GID,\n" +
				"                    p.APP_NAME AS APP_NAME, \n" +
				"                    p.TABLE_NAME AS TABLE_NAME, \n" +
				"                    p.SUBSCRIBE_TYPE AS SUBSCRIBE_TYPE,\n" +
				"                    b.UUID AS UUID,\n" +
				"                    b.BL_NO AS BL_NO, \n" +
				"                    b.MASTER_BL_NO AS MASTER_BL_NO, \n" +
				"                    b.VSL_IMO_NO AS VSL_IMO_NO, \n" +
				"                    b.VSL_NAME AS VSL_NAME, \n" +
				"                    b.VOYAGE AS VOYAGE, \n" +
				"                    b.ACCURATE_IMONO AS ACCURATE_IMONO, \n" +
				"                    b.ACCURATE_VSLNAME AS ACCURATE_VSLNAME, \n" +
				"                    b.I_E_MARK AS I_E_MARK, \n" +
				"                    b.BIZ_STAGE_NO AS BIZ_STAGE_NO, \n" +
				"                    b.BIZ_STAGE_CODE AS BIZ_STAGE_CODE, \n" +
				"                    b.BIZ_STAGE_NAME AS BIZ_STAGE_NAME, \n" +
				"                    b.BIZ_TIME AS BIZ_TIME, \n" +
				"                    b.BIZ_STATUS_CODE AS BIZ_STATUS_CODE, \n" +
				"                    b.BIZ_STATUS AS BIZ_STATUS, \n" +
				"                    b.BIZ_STATUS_IFFECTIVE AS BIZ_STATUS_IFFECTIVE, \n" +
				"                    b.BIZ_STATUS_DESC AS BIZ_STATUS_DESC, \n" +
				"                    b.LASTUPDATEDDT AS LASTUPDATEDDT, \n" +
				"                    CAST(b.ISDELETED AS INT) AS ISDELETED \n" +
				"                  FROM BILL AS b \n" +
				"                    LEFT JOIN ORACLE_SUBSCRIBE_PARAM FOR SYSTEM_TIME as OF b.`proctime` AS p \n" +
				"                      ON p.APP_NAME = 'DATA_FLINK_FULL_FLINK_TRACING_CUSCHK' AND p.TABLE_NAME = 'DM.TRACK_BIZ_STATUS_BILL' \n" +
				"                      WHERE p.ISCURRENT = 1 AND p.SUBSCRIBE_TYPE = 'I' -- 1表示允许输出到下游；1.0 = 1 \n" +
				"                ) t1");
		
//		statementSet.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW CTNR ( \n" +
				"                  UUID, GID,\n" +
				"                  CTNR_NO, \n" +
				"                  VSL_IMO_NO, \n" +
				"                  VSL_NAME, \n" +
				"                  VOYAGE, \n" +
				"                  ACCURATE_IMONO, \n" +
				"                  ACCURATE_VSLNAME, \n" +
				"                  I_E_MARK, \n" +
				"                  BIZ_STAGE_NO, \n" +
				"                  BIZ_STAGE_CODE, \n" +
				"                  BIZ_STAGE_NAME, \n" +
				"                  BIZ_TIME, \n" +
				"                  BIZ_STATUS_CODE, \n" +
				"                  BIZ_STATUS, \n" +
				"                  BIZ_STATUS_IFFECTIVE, \n" +
				"                  BIZ_STATUS_DESC, \n" +
				"                  LASTUPDATEDDT, \n" +
				"                  ISDELETED, \n" +
				"                  `proctime` \n" +
				"                ) AS \n" +
				"                SELECT \n" +
				"                  B.UUID, concat(CI.GID,',',B.GID) AS GID,\n" +
				"                  CI.CTNNO, \n" +
				"                  IF(B.VSL_IMO_NO <> '', B.VSL_IMO_NO, 'N/A'), \n" +
				"                  IF(B.VSL_NAME <> '', B.VSL_NAME, 'N/A'), \n" +
				"                  IF(B.VOYAGE <> '', B.VOYAGE, 'N/A'), \n" +
				"                  IF(B.ACCURATE_IMONO <> '', B.ACCURATE_IMONO, 'N/A'), \n" +
				"                  IF(B.ACCURATE_VSLNAME <> '', B.ACCURATE_VSLNAME, 'N/A'), \n" +
				"                  IF(B.I_E_MARK <> '', B.I_E_MARK, 'N/A'), \n" +
				"                  IF(B.BIZ_STAGE_NO <> '', B.BIZ_STAGE_NO, 'N/A'), \n" +
				"                  IF(B.BIZ_STAGE_CODE <> '', B.BIZ_STAGE_CODE, 'N/A'), \n" +
				"                  IF(B.BIZ_STAGE_NAME <> '', B.BIZ_STAGE_NAME, 'N/A'), \n" +
				"                  CAST(B.BIZ_TIME AS TIMESTAMP(3)), \n" +
				"                  IF(B.BIZ_STATUS_CODE <> '', B.BIZ_STATUS_CODE, 'N/A'), \n" +
				"                  IF(B.BIZ_STATUS <> '', B.BIZ_STATUS, 'N/A'), \n" +
				"                  B.BIZ_STATUS_IFFECTIVE, \n" +
				"                  B.BIZ_STATUS_DESC, -- 不做判断，默认值用 '' \n" +
				"                  CAST(CI.`proctime` AS TIMESTAMP(3)) AS LASTUPDATEDDT, -- PROCTIME() 无法识别时区 \n" +
				"                  B.ISDELETED, \n" +
				"                  CI.`proctime` AS `proctime` \n" +
				"                FROM CTNR_INFO AS CI \n" +
				"                  JOIN BILL AS B ON CI.PARENTLOGID = B.MSGLOGID");
		
		
		statementSet.addInsertSql("" +
				"INSERT INTO ORACLE_TRACK_BIZ_STATUS_CTNR ( \n" +
				"                    UUID, \n" +
				"                    CTNR_NO, \n" +
				"                    VSL_IMO_NO, \n" +
				"                    VSL_NAME, \n" +
				"                    VOYAGE, \n" +
				"                    ACCURATE_IMONO, \n" +
				"                    ACCURATE_VSLNAME, \n" +
				"                    I_E_MARK, \n" +
				"                    BIZ_STAGE_NO, \n" +
				"                    BIZ_STAGE_CODE, \n" +
				"                    BIZ_STAGE_NAME, \n" +
				"                    BIZ_TIME, \n" +
				"                    BIZ_STATUS_CODE, \n" +
				"                    BIZ_STATUS, \n" +
				"                    BIZ_STATUS_IFFECTIVE, \n" +
				"                    BIZ_STATUS_DESC, \n" +
				"                    LASTUPDATEDDT, \n" +
				"                    ISDELETED \n" +
				"                ) \n" +
				"                SELECT \n" +
				"                  CTNR.UUID, \n" +
				"                  CTNR.CTNR_NO, \n" +
				"                  CTNR.VSL_IMO_NO, \n" +
				"                  CTNR.VSL_NAME, \n" +
				"                  CTNR.VOYAGE, \n" +
				"                  CTNR.ACCURATE_IMONO, \n" +
				"                  CTNR.ACCURATE_VSLNAME, \n" +
				"                  CTNR.I_E_MARK, \n" +
				"                  CTNR.BIZ_STAGE_NO, \n" +
				"                  CTNR.BIZ_STAGE_CODE, \n" +
				"                  CTNR.BIZ_STAGE_NAME, \n" +
				"                  CTNR.BIZ_TIME, \n" +
				"                  CTNR.BIZ_STATUS_CODE, \n" +
				"                  CTNR.BIZ_STATUS, \n" +
				"                  CTNR.BIZ_STATUS_IFFECTIVE, \n" +
				"                  CTNR.BIZ_STATUS_DESC, \n" +
				"                  CTNR.LASTUPDATEDDT, \n" +
				"                  CTNR.ISDELETED \n" +
				"                FROM CTNR left join ORACLE_CTNR_DIM FOR SYSTEM_TIME as OF CTNR.`proctime` as ocd  \n" +
				"                  on CTNR.VSL_IMO_NO=ocd.VSL_IMO_NO \n" +
				"                  and CTNR.VOYAGE=ocd.VOYAGE \n" +
				"                  and CTNR.CTNR_NO=ocd.CTNR_NO \n" +
				"                  and CTNR.BIZ_STAGE_NO=ocd.BIZ_STAGE_NO \n" +
				"                  and CTNR.BIZ_STATUS_CODE=ocd.BIZ_STATUS_CODE \n" +
				"                where (ocd.BIZ_TIME is null or CTNR.BIZ_TIME>ocd.BIZ_TIME) \n" +
				"                  and CTNR.BIZ_TIME is not null");
		
		
		
		statementSet.addInsertSql("" +
				"INSERT INTO KAFKA_TRACK_BIZ_STATUS_CTNR (\n" +
				"                  GID,\n" +
				"                  APP_NAME,\n" +
				"                  TABLE_NAME,\n" +
				"                  SUBSCRIBE_TYPE,\n" +
				"                  DATA\n" +
				"                )\n" +
				"                SELECT\n" +
				"                  GID,\n" +
				"                  APP_NAME,\n" +
				"                  TABLE_NAME,\n" +
				"                  SUBSCRIBE_TYPE,\n" +
				"                  ROW(UUID, CTNR_NO, VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE,\n" +
				"                    BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_IFFECTIVE,BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED) AS DATA -- 此时的 LASTUPDATEDDT 没有  '.'\n" +
				"                FROM (\n" +
				"                  SELECT\n" +
				"                    c.GID AS GID,\n" +
				"                    p.APP_NAME AS APP_NAME,\n" +
				"                    p.TABLE_NAME AS TABLE_NAME,\n" +
				"                    p.SUBSCRIBE_TYPE AS SUBSCRIBE_TYPE,\n" +
				"                    c.UUID AS UUID,\n" +
				"                    c.CTNR_NO AS CTNR_NO,\n" +
				"                    c.VSL_IMO_NO AS VSL_IMO_NO,\n" +
				"                    c.VSL_NAME AS VSL_NAME,\n" +
				"                    c.VOYAGE AS VOYAGE,\n" +
				"                    c.ACCURATE_IMONO AS ACCURATE_IMONO,\n" +
				"                    c.ACCURATE_VSLNAME AS ACCURATE_VSLNAME,\n" +
				"                    c.I_E_MARK AS I_E_MARK,\n" +
				"                    c.BIZ_STAGE_NO AS BIZ_STAGE_NO,\n" +
				"                    c.BIZ_STAGE_CODE AS BIZ_STAGE_CODE,\n" +
				"                    c.BIZ_STAGE_NAME AS BIZ_STAGE_NAME,\n" +
				"                    c.BIZ_TIME AS BIZ_TIME,\n" +
				"                    c.BIZ_STATUS_CODE AS BIZ_STATUS_CODE,\n" +
				"                    c.BIZ_STATUS AS BIZ_STATUS,\n" +
				"                    c.BIZ_STATUS_IFFECTIVE AS BIZ_STATUS_IFFECTIVE,\n" +
				"                    c.BIZ_STATUS_DESC AS BIZ_STATUS_DESC,\n" +
				"                    c.LASTUPDATEDDT AS LASTUPDATEDDT, -- 重命名以确保后续使用 ROW 时不需要 '.'\n" +
				"                    CAST(c.ISDELETED AS INT) AS ISDELETED -- 自动转换类型\n" +
				"                  FROM CTNR AS c\n" +
				"                    LEFT JOIN ORACLE_SUBSCRIBE_PARAM FOR SYSTEM_TIME as OF c.`proctime` AS p\n" +
				"                      ON 'DATA_FLINK_FULL_FLINK_TRACING_CUSCHK'=p.APP_NAME AND 'DM.TRACK_BIZ_STATUS_CTNR'=p.TABLE_NAME\n" +
				"                        WHERE p.ISCURRENT = 1 AND p.SUBSCRIBE_TYPE = 'I'\n" +
				"                ) t");
		
		statementSet.execute();
		
	}
}
