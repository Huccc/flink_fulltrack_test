package com.chao.flinksql.departure;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class departure2 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO Kafka数据源
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data(\n" +
				"msgId       STRING,\n" +
				"bizId       STRING,\n" +
				"msgType     STRING,\n" +
				"bizUniqueId STRING,\n" +
				"destination STRING,\n" +
				"parseData   STRING,\n" +
				"LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'DYN_DEP_DECLARE',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'DYN_DEP_DECLARE',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				"  )");
		
		// TODO 船舶状态表维表
		tEnv.executeSql("" +
				"create table track_biz_status_ship(\n" +
				"   VSL_IMO_NO       String,\n" +
				"   VSL_NAME         String,\n" +
				"   VOYAGE_IN        String,\n" +
				"   VOYAGE_OUT       String,\n" +
				"   CUS_CUSTOMS_CODE String,\n" +
				"   CUS_CUSTOMS_NAME String,\n" +
				"   MSA_DOCK_CODE    String,\n" +
				"   MSA_DOCK_NAME    String,\n" +
				"   MSA_BERTH_CODE   String,\n" +
				"   MSA_BERTH_NAME   String,\n" +
				"   DATA_SOURCE      String,\n" +
				"   BIZ_STAGE_NO     String,\n" +
				"   BIZ_STAGE_CODE   String,\n" +
				"   BIZ_STAGE_NAME   String,\n" +
				"   BIZ_TIME         TIMESTAMP,\n" +
				"   BIZ_STATUS_CODE  String,\n" +
				"   BIZ_STATUS       String,\n" +
				"   BIZ_STATUS_DESC  String,\n" +
				"   LASTUPDATEDDT    TIMESTAMP,\n" +
				"   ISDELETED        DECIMAL(22, 0),\n" +
				"   PRIMARY KEY (VSL_IMO_NO, VOYAGE_IN, VOYAGE_OUT, BIZ_STAGE_NO) NOT ENFORCED\n" +
				") with (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_SHIP',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				"  )");
		
		// TODO REDIS 维表
		tEnv.executeSql("" +
				"CREATE TABLE redis_dim(\n" +
				"key     String,\n" +
				"hashkey String,\n" +
				"res     String\n" +
				") WITH (\n" +
				"  'connector.type' = 'redis',\n" +
				"  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
				"  'database.num' = '0',\n" +
				"  'operate.type' = 'hash',\n" +
				"  'redis.version' = '2.6'\n" +
				"  )");
		
		// TODO Oracle配置表 维表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_subscribe_papam_dim\n" +
				"(\n" +
				"   APP_NAME         STRING,\n" +
				"   TABLE_NAME       STRING,\n" +
				"   SUBSCRIBE_TYPE   STRING,\n" +
				"   SUBSCRIBER       STRING,\n" +
				"   DB_URL           STRING,\n" +
				"   KAFKA_SERVERS    STRING,\n" +
				"   KAFKA_SINK_TOPIC STRING,\n" +
				"   ISCURRENT        DECIMAL(10, 0),\n" +
				"   LASTUPDATEDDT    TIMESTAMP(3),\n" +
				"   PRIMARY KEY (APP_NAME, TABLE_NAME) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
				"  'username' = 'adm_bdpp',\n" +
				"  'password' = 'easipass',\n" +
				"  'lookup.cache.max-rows' = '200',\n" +
				"  'lookup.cache.ttl' = '60', -- 单位 s\n" +
				"  'lookup.max-retries' = '3'\n" +
				"  )");
		
		// TODO Oracle提单状态表
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_TRACK_BIZ_STATUS_BILL(\n" +
				"   VSL_IMO_NO           STRING,\n" +
				"   VSL_NAME             STRING,\n" +
				"   VOYAGE               STRING,\n" +
				"   ACCURATE_IMONO       STRING,\n" +
				"   ACCURATE_VSLNAME     STRING,\n" +
				"   BL_NO                STRING,\n" +
				"   MASTER_BL_NO         STRING,\n" +
				"   I_E_MARK             STRING,\n" +
				"   BIZ_STAGE_NO         STRING,\n" +
				"   BIZ_STAGE_CODE       STRING,\n" +
				"   BIZ_STAGE_NAME       STRING,\n" +
				"   BIZ_TIME             TIMESTAMP,\n" +
				"   BIZ_STATUS_CODE      STRING,\n" +
				"   BIZ_STATUS           STRING,\n" +
				"   BIZ_STATUS_DESC      STRING,\n" +
				"   LASTUPDATEDDT        TIMESTAMP,\n" +
				"   ISDELETED            DECIMAL(22, 0),\n" +
				"   UUID                 STRING,\n" +
				"   BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"   PRIMARY KEY (VSL_IMO_NO, VOYAGE, BL_NO, BIZ_STAGE_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				"  )");
		
		// TODO Kafka提单表
		tEnv.executeSql("" +
				"create table kafka_bill(\n" +
				"   GID            STRING,\n" +
				"   APP_NAME       STRING,\n" +
				"   TABLE_NAME     STRING,\n" +
				"   SUBSCRIBE_TYPE STRING,\n" +
				"   DATA           ROW(\n" +
				"       VSL_IMO_NO STRING,\n" +
				"       VSL_NAME STRING,\n" +
				"       VOYAGE STRING,\n" +
				"       ACCURATE_IMONO STRING,\n" +
				"       ACCURATE_VSLNAME STRING,\n" +
				"       BL_NO STRING,\n" +
				"       MASTER_BL_NO STRING,\n" +
				"       I_E_MARK STRING,\n" +
				"       BIZ_STAGE_NO STRING,\n" +
				"       BIZ_STAGE_CODE STRING,\n" +
				"       BIZ_STAGE_NAME STRING,\n" +
				"       BIZ_TIME TIMESTAMP (3),\n" +
				"       BIZ_STATUS_CODE STRING,\n" +
				"       BIZ_STATUS STRING,\n" +
				"       BIZ_STATUS_DESC STRING,\n" +
				"       LASTUPDATEDDT TIMESTAMP (3),\n" +
				"       ISDELETED int,\n" +
				"       UUID STRING,\n" +
				"       BIZ_STATUS_IFFECTIVE int\n" +
				"    )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'LEAVE_PORT_BILL',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'format' = 'json'\n" +
				"  )");
		
		// TODO Oracle箱状态表
		tEnv.executeSql("" +
				"CREATE TABLE ORACLE_TRACK_BIZ_STATUS_CTNR (\n" +
				"    VSL_IMO_NO STRING,\n" +
				"    VSL_NAME STRING,\n" +
				"    VOYAGE STRING,\n" +
				"    ACCURATE_IMONO STRING,\n" +
				"    ACCURATE_VSLNAME STRING,\n" +
				"    CTNR_NO STRING,\n" +
				"    I_E_MARK STRING,\n" +
				"    BIZ_STAGE_NO STRING,\n" +
				"    BIZ_STAGE_CODE STRING,\n" +
				"    BIZ_STAGE_NAME STRING,\n" +
				"    BIZ_TIME TIMESTAMP,\n" +
				"    BIZ_STATUS_CODE STRING,\n" +
				"    BIZ_STATUS STRING,\n" +
				"    BIZ_STATUS_DESC STRING,\n" +
				"    LASTUPDATEDDT TIMESTAMP,\n" +
				"    ISDELETED DECIMAL(22, 0),\n" +
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"    PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO Kafka 箱状态表
		tEnv.executeSql("" +
				"create table kafka_ctn(\n" +
				"  GID STRING,\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING,\n" +
				"  DATA ROW(\n" +
				"      VSL_IMO_NO STRING,\n" +
				"      VSL_NAME STRING,\n" +
				"      VOYAGE STRING,\n" +
				"      ACCURATE_IMONO STRING,\n" +
				"      ACCURATE_VSLNAME STRING,\n" +
				"      CTNR_NO STRING,\n" +
				"      I_E_MARK STRING,\n" +
				"      BIZ_STAGE_NO STRING,\n" +
				"      BIZ_STAGE_CODE STRING,\n" +
				"      BIZ_STAGE_NAME STRING,\n" +
				"      BIZ_TIME TIMESTAMP(3),\n" +
				"      BIZ_STATUS_CODE STRING,\n" +
				"      BIZ_STATUS STRING,\n" +
				"      BIZ_STATUS_DESC STRING,\n" +
				"      LASTUPDATEDDT TIMESTAMP(3),\n" +
				"      ISDELETED int,\n" +
				"      UUID STRING,\n" +
				"      BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'LEAVE_PORT_CTNR',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO 从Kafka获取数据
		tEnv.executeSql("" +
				"CREATE VIEW TMP_DEP_TRACK_INFO(\n" +
				" `map`,\n" +
				" LASTUPDATEDDT, GID\n" +
				"    )AS\n" +
				"SELECT STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',',':')AS `map`,\n" +
				"       LASTUPDATEDDT,\n" +
				"       concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
				"FROM kafka_source_data\n" +
				"WHERE bizId = 'ogg_data'\n" +
				"  AND destination = 'SRC_SHIPDYN.DYN_DEP_DECLARE'");

//		Table TMP_DEP_TRACK_INFO = tEnv.sqlQuery("select * from TMP_DEP_TRACK_INFO");
//		tEnv.toAppendStream(TMP_DEP_TRACK_INFO, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW TRACK_DEP_INFO(\n" +
				" ID,\n" +
				" IMO_NO,\n" +
				" VESSELNAME_EN,\n" +
				" VOYAGE_IN,\n" +
				" VOYAGE_OUT,\n" +
				" DEPARTURE_DATE,\n" +
				" LASTUPDATEDDT, GID\n" +
				" ) AS\n" +
				"SELECT `map`['ID'],\n" +
				"       `map`['IMO_NO'],\n" +
				"       `map`['VESSELNAME_EN'],\n" +
				"       `map`['VOYAGE_IN'],\n" +
				"       `map`['VOYAGE_OUT'],\n" +
				"       `map`['DEPARTURE_DATE'],\n" +
				"       LASTUPDATEDDT,\n" +
				"       GID\n" +
				"FROM TMP_DEP_TRACK_INFO");

		Table TRACK_DEP_INFO = tEnv.sqlQuery("select * from TRACK_DEP_INFO");
		tEnv.toAppendStream(TRACK_DEP_INFO, Row.class).print();
//		env.execute();
		
		// TODO 匹配提单表维表
		tEnv.executeSql("" +
				"create view BILL_INFO AS\n" +
				"select dim1.BL_NO,\n" +
				"       dim1.MASTER_BL_NO,\n" +
				"       dim1.VSL_IMO_NO,\n" +
				"       dim1.VSL_NAME,\n" +
				"       dim1.VOYAGE,\n" +
				"       dim1.ACCURATE_IMONO,\n" +
				"       dim1.ACCURATE_VSLNAME,\n" +
				"       dim1.I_E_MARK,\n" +
				"       dim1.BIZ_STAGE_NO,\n" +
				"       dim1.BIZ_STAGE_CODE,\n" +
				"       dim1.BIZ_STAGE_NAME,\n" +
				"       dim1.BIZ_STATUS_CODE,\n" +
				"       dim1.BIZ_STATUS,\n" +
				"       dim1.BIZ_STATUS_DESC,\n" +
				"       if(UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_IN, '[\\t\\n\\r]', ''))) <> '',\n" +
				"          UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_IN, '[\\t\\n\\r]', ''))), 'N/A')  as VOYAGE_IN,\n" +
				"       if(UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', ''))) <> '',\n" +
				"          UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE_OUT,\n" +
				"       TDI.GID,\n" +
				"       TO_TIMESTAMP(TDI.DEPARTURE_DATE,'yyyy-MM-dd HH:mm:ss') as BIZ_TIME,\n" +
				"       TDI.LASTUPDATEDDT\n" +
				"from TRACK_DEP_INFO as TDI\n" +
				"     left join ORACLE_TRACK_BIZ_STATUS_BILL FOR SYSTEM_TIME AS OF TDI.LASTUPDATEDDT AS dim1\n" +
				"               on dim1.VSL_IMO_NO = REPLACE(UPPER(TRIM(REGEXP_REPLACE(TDI.IMO_NO, '[\\t\\n\\r]', ''))), 'UN', '') and\n" +
				"                  dim1.VSL_NAME = UPPER(TRIM(REGEXP_REPLACE(TDI.VESSELNAME_EN, '[\\t\\n\\r]', '')))\n" +
				"where dim1.BIZ_STATUS = '已装船'\n" +
				"  AND dim1.BIZ_STAGE_NAME = '装船'\n" +
				"  AND dim1.VOYAGE = UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', '')))" +
				"");

		Table BILL_INFO = tEnv.sqlQuery("select * from BILL_INFO");
		tEnv.toAppendStream(BILL_INFO, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view BILL AS\n" +
				"select BI.GID,\n" +
				"       BI.VSL_IMO_NO,\n" +
				"       BI.VSL_NAME,\n" +
				"       BI.VOYAGE,\n" +
				"       BI.ACCURATE_IMONO,\n" +
				"       BI.ACCURATE_VSLNAME,\n" +
				"       BI.BL_NO,\n" +
				"       BI.MASTER_BL_NO,\n" +
				"       BI.I_E_MARK,\n" +
				"       CASE BI.BIZ_STAGE_NO\n" +
				"       WHEN 'C8.4' THEN 'C8.6'\n" +
				"       WHEN 'C8.4s' THEN 'C8.6s'\n" +
				"       ELSE 'N/A' END                  AS BIZ_STAGE_NO,   --区分集装箱货和散货\n" +
				"       CASE BI.BIZ_STAGE_NO\n" +
				"       WHEN 'C8.4' THEN 'E_vslDep_dyn'\n" +
				"       WHEN 'C8.4s' THEN 'E_vslDep_dynBulk'\n" +
				"       ELSE 'N/A' END                  AS BIZ_STAGE_CODE, --区分集装箱货和散货\n" +
				"       if(dim2.res <> '', dim2.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"       BI.BIZ_TIME,\n" +
				"       '1'                                 AS BIZ_STATUS_CODE,\n" +
				"       '已离港'                               AS BIZ_STATUS,\n" +
				"       'N/A'                               AS BIZ_STATUS_DESC,\n" +
				"       BI.LASTUPDATEDDT,\n" +
				"       0                                   as ISDELETED,\n" +
				"       uuid()                              AS UUID,\n" +
				"       1                                   AS BIZ_STATUS_IFFECTIVE\n" +
				"from BILL_INFO as BI\n" +
				"     left join redis_dim FOR SYSTEM_TIME AS OF BI.LASTUPDATEDDT AS dim2\n" +
				"               ON concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',\n" +
				"                         CASE BI.BIZ_STAGE_NO WHEN 'C8.4' THEN 'C8.6' WHEN 'C8.4s' THEN 'C8.6s' ELSE 'N/A' END,\n" +
				"                         '&SUB_STAGE_CODE=', CASE BI.BIZ_STAGE_NO\n" +
				"                                             WHEN 'C8.4' THEN 'E_vslDep_dyn'\n" +
				"                                             WHEN 'C8.4s' THEN 'E_vslDep_dynBulk'\n" +
				"                                             ELSE 'N/A' END) = dim2.key and 'SUB_STAGE_NAME' = dim2.hashkey\n");

		Table BILL = tEnv.sqlQuery("select * from BILL");
		tEnv.toAppendStream(BILL, Row.class).print();
//		env.execute();
		
		// TODO 匹配箱表维表
		tEnv.executeSql("" +
				"create view CTNR_INFO AS\n" +
				"select dim1.CTNR_NO,\n" +
				"       dim1.VSL_IMO_NO,\n" +
				"       dim1.VSL_NAME,\n" +
				"       dim1.VOYAGE,\n" +
				"       dim1.ACCURATE_IMONO,\n" +
				"       dim1.ACCURATE_VSLNAME,\n" +
				"       dim1.I_E_MARK,\n" +
				"       dim1.BIZ_STAGE_NO,\n" +
				"       dim1.BIZ_STAGE_CODE,\n" +
				"       dim1.BIZ_STAGE_NAME,\n" +
				"       dim1.BIZ_STATUS_CODE,\n" +
				"       dim1.BIZ_STATUS,\n" +
				"       dim1.BIZ_STATUS_DESC,\n" +
				"       if(UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_IN, '[\\t\\n\\r]', ''))) <> '',\n" +
				"          UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_IN, '[\\t\\n\\r]', ''))), 'N/A')  as VOYAGE_IN,\n" +
				"       if(UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', ''))) <> '',\n" +
				"          UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE_OUT,\n" +
				"       TDI.GID,\n" +
				"       TO_TIMESTAMP(TDI.DEPARTURE_DATE,'yyyy-MM-dd HH:mm:ss') as BIZ_TIME,\n" +
				"       TDI.LASTUPDATEDDT\n" +
				"from TRACK_DEP_INFO as TDI\n" +
				"     left join ORACLE_TRACK_BIZ_STATUS_CTNR FOR SYSTEM_TIME AS OF TDI.LASTUPDATEDDT AS dim1\n" +
				"               on dim1.VSL_IMO_NO = REPLACE(UPPER(TRIM(REGEXP_REPLACE(TDI.IMO_NO, '[\\t\\n\\r]', ''))), 'UN', '') and\n" +
				"                  dim1.VSL_NAME = UPPER(TRIM(REGEXP_REPLACE(TDI.VESSELNAME_EN, '[\\t\\n\\r]', '')))\n" +
				"where dim1.BIZ_STATUS = '已装船'\n" +
				"  AND dim1.BIZ_STAGE_NAME = '装船'\n" +
				"  AND dim1.VOYAGE = UPPER(TRIM(REGEXP_REPLACE(TDI.VOYAGE_OUT, '[\\t\\n\\r]', '')))");
		
//		Table CTNR_INFO = tEnv.sqlQuery("select * from CTNR_INFO");
//		tEnv.toAppendStream(CTNR_INFO, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view ctnrTB AS\n" +
				"select CI.GID,\n" +
				"       CI.VSL_IMO_NO,\n" +
				"       CI.VSL_NAME,\n" +
				"       CI.VOYAGE,\n" +
				"       CI.ACCURATE_IMONO,\n" +
				"       CI.ACCURATE_VSLNAME,\n" +
				"       CI.CTNR_NO,\n" +
				"       CI.I_E_MARK,\n" +
				"       'C8.6'                              AS BIZ_STAGE_NO,   --区分集装箱货和散货\n" +
				"       'E_vslDep_dyn'                      AS BIZ_STAGE_CODE, --区分集装箱货和散货\n" +
				"       if(dim2.res <> '', dim2.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"       CI.BIZ_TIME,\n" +
				"       '1'                                 AS BIZ_STATUS_CODE,\n" +
				"       '已离港'                               AS BIZ_STATUS,\n" +
				"       'N/A'                               AS BIZ_STATUS_DESC,\n" +
				"       CI.LASTUPDATEDDT,\n" +
				"       0                                   as ISDELETED,\n" +
				"       uuid()                              AS UUID,\n" +
				"       1                                   AS BIZ_STATUS_IFFECTIVE\n" +
				"from CTNR_INFO as CI\n" +
				"     left join redis_dim FOR SYSTEM_TIME AS OF CI.LASTUPDATEDDT AS dim2\n" +
				"               ON concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=', 'C8.6', '&SUB_STAGE_CODE=', 'E_vslDep_dyn') =\n" +
				"                  dim2.key and 'SUB_STAGE_NAME' = dim2.hashkey\n");
		
		Table ctnrTB = tEnv.sqlQuery("select * from ctnrTB");
		tEnv.toAppendStream(ctnrTB, Row.class).print();
//		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 写入Oracle提单状态表
		statementSet.addInsertSql("" +
				"insert into ORACLE_TRACK_BIZ_STATUS_BILL(VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, BL_NO,\n" +
				"                                         MASTER_BL_NO, I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME,\n" +
				"                                         BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED, UUID,\n" +
				"                                         BIZ_STATUS_IFFECTIVE)\n" +
				"select BILL.VSL_IMO_NO,\n" +
				"       BILL.VSL_NAME,\n" +
				"       BILL.VOYAGE,\n" +
				"       BILL.ACCURATE_IMONO,\n" +
				"       BILL.ACCURATE_VSLNAME,\n" +
				"       BILL.BL_NO,\n" +
				"       BILL.MASTER_BL_NO,\n" +
				"       BILL.I_E_MARK,\n" +
				"       BILL.BIZ_STAGE_NO,\n" +
				"       BILL.BIZ_STAGE_CODE,\n" +
				"       BILL.BIZ_STAGE_NAME,\n" +
				"       BILL.BIZ_TIME,\n" +
				"       BILL.BIZ_STATUS_CODE,\n" +
				"       BILL.BIZ_STATUS,\n" +
				"       BILL.BIZ_STATUS_DESC,\n" +
				"       cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
				"       BILL.ISDELETED,\n" +
				"       BILL.UUID,\n" +
				"       BILL.BIZ_STATUS_IFFECTIVE\n" +
				"from BILL\n" +
				"     left join ORACLE_TRACK_BIZ_STATUS_BILL FOR SYSTEM_TIME as OF BILL.LASTUPDATEDDT as obd\n" +
				"               on BILL.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
				"               and BILL.VOYAGE = obd.VOYAGE\n" +
				"               and BILL.BL_NO = obd.BL_NO\n" +
				"               and BILL.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or BILL.BIZ_TIME > obd.BIZ_TIME) --前者插入后者更新\n" +
				"  and BILL.BIZ_TIME is not null");
		
		// TODO 写入Oracle箱状态表
		statementSet.addInsertSql("" +
				"insert into ORACLE_TRACK_BIZ_STATUS_CTNR(VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, CTNR_NO,\n" +
				"                                         I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME,\n" +
				"                                         BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED, UUID,\n" +
				"                                         BIZ_STATUS_IFFECTIVE)\n" +
				"select ctnrTB.VSL_IMO_NO,\n" +
				"       ctnrTB.VSL_NAME,\n" +
				"       ctnrTB.VOYAGE,\n" +
				"       ctnrTB.ACCURATE_IMONO,\n" +
				"       ctnrTB.ACCURATE_VSLNAME,\n" +
				"       ctnrTB.CTNR_NO,\n" +
				"       ctnrTB.I_E_MARK,\n" +
				"       ctnrTB.BIZ_STAGE_NO,\n" +
				"       ctnrTB.BIZ_STAGE_CODE,\n" +
				"       ctnrTB.BIZ_STAGE_NAME,\n" +
				"       ctnrTB.BIZ_TIME,\n" +
				"       ctnrTB.BIZ_STATUS_CODE,\n" +
				"       ctnrTB.BIZ_STATUS,\n" +
				"       ctnrTB.BIZ_STATUS_DESC,\n" +
				"       cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
				"       ctnrTB.ISDELETED,\n" +
				"       ctnrTB.UUID,\n" +
				"       ctnrTB.BIZ_STATUS_IFFECTIVE\n" +
				"from ctnrTB\n" +
				"     left join ORACLE_TRACK_BIZ_STATUS_CTNR FOR SYSTEM_TIME as OF ctnrTB.LASTUPDATEDDT as ocd\n" +
				"               on ctnrTB.VSL_IMO_NO = ocd.VSL_IMO_NO\n" +
				"               and ctnrTB.VOYAGE = ocd.VOYAGE\n" +
				"               and ctnrTB.CTNR_NO = ocd.CTNR_NO\n" +
				"               and ctnrTB.BIZ_STAGE_NO = ocd.BIZ_STAGE_NO\n" +
				"where (ocd.BIZ_TIME is null or ctnrTB.BIZ_TIME > ocd.BIZ_TIME) --前者插入后者更新\n" +
				"  and ctnrTB.BIZ_TIME is not null");
		
		// TODO 写入Kafka提单状态表
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID, APP_NAME, TABLE_NAME, SUBSCRIBE_TYPE, DATA)\n" +
				"select GID,\n" +
				"       'DATA_FLINK_FULL_FLINK_TRACING_LEAVE_PORT' as APP_NAME,\n" +
				"       'DM.TRACK_BIZ_STATUS_BILL'                 as TABLE_NAME,\n" +
				"       'I'                                        as SUBSCRIBE_TYPE,\n" +
				"       ROW(VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, BL_NO, MASTER_BL_NO, I_E_MARK, BIZ_STAGE_NO,\n" +
				"           BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT,\n" +
				"           ISDELETED, UUID, BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from (select bc1.GID                              as GID,\n" +
				"             bc1.VSL_IMO_NO,\n" +
				"             bc1.VSL_NAME,\n" +
				"             bc1.VOYAGE,\n" +
				"             bc1.ACCURATE_IMONO,\n" +
				"             bc1.ACCURATE_VSLNAME,\n" +
				"             bc1.BL_NO,\n" +
				"             bc1.MASTER_BL_NO,\n" +
				"             bc1.I_E_MARK,\n" +
				"             bc1.BIZ_STAGE_NO,\n" +
				"             bc1.BIZ_STAGE_CODE,\n" +
				"             bc1.BIZ_STAGE_NAME,\n" +
				"             bc1.BIZ_TIME,\n" +
				"             bc1.BIZ_STATUS_CODE,\n" +
				"             bc1.BIZ_STATUS,\n" +
				"             bc1.BIZ_STATUS_DESC,\n" +
				"             cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
				"             bc1.ISDELETED,\n" +
				"             bc1.UUID,\n" +
				"             bc1.BIZ_STATUS_IFFECTIVE\n" +
				"      from BILL as bc1\n" +
				"           left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bc1.LASTUPDATEDDT as ospd\n" +
				"                     on 'DATA_FLINK_FULL_FLINK_TRACING_LEAVE_PORT' = ospd.APP_NAME\n" +
				"                     AND 'DM.TRACK_BIZ_STATUS_BILL' = ospd.TABLE_NAME\n" +
				"      where ospd.ISCURRENT = 1\n" +
				"    and bc1.BIZ_TIME is not null\n" +
				"     ) as bc2");
		
		// TODO 写入Kafka箱状态表
		statementSet.addInsertSql("" +
				"insert into kafka_ctn (GID, APP_NAME, TABLE_NAME, SUBSCRIBE_TYPE, DATA)\n" +
				"select GID,\n" +
				"       'DATA_FLINK_FULL_FLINK_TRACING_LEAVE_PORT' as APP_NAME,\n" +
				"       'DM.TRACK_BIZ_STATUS_CTNR'                 as TABLE_NAME,\n" +
				"       'I'                                        as SUBSCRIBE_TYPE,\n" +
				"       ROW(VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, CTNR_NO, I_E_MARK, BIZ_STAGE_NO,\n" +
				"           BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT,\n" +
				"           ISDELETED, UUID, BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from (select ct1.GID                              as GID,\n" +
				"             ct1.VSL_IMO_NO,\n" +
				"             ct1.VSL_NAME,\n" +
				"             ct1.VOYAGE,\n" +
				"             ct1.ACCURATE_IMONO,\n" +
				"             ct1.ACCURATE_VSLNAME,\n" +
				"             ct1.CTNR_NO,\n" +
				"             ct1.I_E_MARK,\n" +
				"             ct1.BIZ_STAGE_NO,\n" +
				"             ct1.BIZ_STAGE_CODE,\n" +
				"             ct1.BIZ_STAGE_NAME,\n" +
				"             ct1.BIZ_TIME,\n" +
				"             ct1.BIZ_STATUS_CODE,\n" +
				"             ct1.BIZ_STATUS,\n" +
				"             ct1.BIZ_STATUS_DESC,\n" +
				"             cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
				"             ct1.ISDELETED,\n" +
				"             ct1.UUID,\n" +
				"             ct1.BIZ_STATUS_IFFECTIVE\n" +
				"      from ctnrTB as ct1\n" +
				"           left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF ct1.LASTUPDATEDDT as ospd\n" +
				"                     on 'DATA_FLINK_FULL_FLINK_TRACING_LEAVE_PORT' = ospd.APP_NAME\n" +
				"                     AND 'DM.TRACK_BIZ_STATUS_CTNR' = ospd.TABLE_NAME\n" +
				"      where ospd.ISCURRENT = 1\n" +
				"    and ct1.BIZ_TIME is not null\n" +
				"     ) as ct2");
		
		statementSet.execute();
	}
}
