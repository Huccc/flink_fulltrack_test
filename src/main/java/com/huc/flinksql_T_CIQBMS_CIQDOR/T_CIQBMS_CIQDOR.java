package com.huc.flinksql_T_CIQBMS_CIQDOR;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class T_CIQBMS_CIQDOR {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO kafka数据源
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-db-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-tciqbmsciqdor',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO kafka配置表 维表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_subscribe_papam_dim (\n" +
				"    APP_NAME STRING,\n" +
				"    TABLE_NAME STRING,\n" +
				"    SUBSCRIBE_TYPE STRING,\n" +
				"    SUBSCRIBER STRING,\n" +
				"    DB_URL STRING,\n" +
				"    KAFKA_SERVERS STRING,\n" +
				"    KAFKA_SINK_TOPIC STRING,\n" +
				"    ISCURRENT DECIMAL(11, 0),\n" +
				"    LASTUPDATEDDT TIMESTAMP,\n" +
				"    PRIMARY KEY(APP_NAME,TABLE_NAME) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass',\n" +
				"  'lookup.cache.max-rows' = '100',\n" +
				"  'lookup.cache.ttl' = '100',\n" +
				"  'lookup.max-retries' = '3'\n" +
				")");
		
		// TODO Oracle提单表，维表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_bill_dim (\n" +
				"    VSL_IMO_NO STRING,\n" +
				"    VSL_NAME STRING,\n" +
				"    VOYAGE STRING,\n" +
				"    ACCURATE_IMONO STRING,\n" +
				"    ACCURATE_VSLNAME STRING,\n" +
				"    BL_NO STRING,\n" +
				"    MASTER_BL_NO STRING,\n" +
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
				"    PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO redis dim
		tEnv.executeSql("" +
				"CREATE TABLE redis_dim (\n" +
				"  key String,\n" +
				"  hashkey String,\n" +
				"  res String\n" +
				") WITH (\n" +
				"  'connector.type' = 'redis',\n" +
				"  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
				"  'database.num' = '0',\n" +
				"  'operate.type' = 'hash',\n" +
				"  'redis.version' = '2.6'\n" +
				")");
		
		// TODO 解析json 1
		// 注册map解析函数
//		tEnv.createTemporarySystemFunction("JsonToMap", JsonToMap.class);
//		tEnv.executeSql("" +
//				"create view sourceTB_func as\n" +
//				"select\n" +
//				"  msgId,LASTUPDATEDDT,JsonToMap(parseData)\n" +
//				"from kafka_source_data\n" +
//				"WHERE\n" +
//				"  bizId = 'ogg_data'\n" +
//				"  AND destination = 'SRC_GCC.T_CIQBMS_CIQDOR'");
		
		// TODO 解析json 2
		tEnv.executeSql("" +
				"create view sourceTB as\n" +
				"select\n" +
				"  msgId,LASTUPDATEDDT,STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') as pData --parseMap(parseData)\n" +
				"from kafka_source_data\n" +
				"WHERE\n" +
				"  bizId = 'ogg_data'\n" +
				"  AND destination = 'SRC_GCC.T_CIQBMS_CIQDOR'");

//		Table sourceTB_table = tEnv.sqlQuery("select * from sourceTB");
//		tEnv.toAppendStream(sourceTB_table, Row.class).print();
//		env.execute();
		
		// TODO 选择字段
		tEnv.executeSql("" +
				"create view commonTB as\n" +
				"select\n" +
				"  msgId,\n" +
				"  if(pData['VESSEL_NAME'] <> '', UPPER(TRIM(REGEXP_REPLACE(pData['VESSEL_NAME'], '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"  if(pData['VOYAGE'] <> '', UPPER(TRIM(REGEXP_REPLACE(pData['VOYAGE'], '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  pData['SUB_BL_NO'] as BL_NO,\n" +
				"  pData['BL_NO'] as MASTER_BL_NO,\n" +
				"  'I' as I_E_MARK,\n" +
				"  'D8.4' as BIZ_STAGE_NO,\n" +
				"  'I_ctnrDeconsolidation_ciqdor' as BIZ_STAGE_CODE,\n" +
				"  TO_TIMESTAMP(pData['OUT_DOOR_TIME'], 'yyyyMMddHHmm') as BIZ_TIME,\n" +
				"  '1' as BIZ_STATUS_CODE,\n" +
				"  'N/A' as BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  0 as ISDELETED\n" +
				"from sourceTB");
		
		Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
		tEnv.toAppendStream(commonTB_table, Row.class).print();
//		env.execute();
		
		// TODO 关联维表
		tEnv.executeSql("" +
				"create view billTB as\n" +
				"select\n" +
				"  msgId,\n" +
				"  if(dim5.res <> '', dim5.res, 'N/A') as VSL_IMO_NO,\n" +
				"  VSL_NAME,\n" +
				"  VOYAGE,\n" +
				"  if(dim5.res <> '', dim5.res, 'N/A') as ACCURATE_IMONO,\n" +
				"  if(dim6.res <> '', dim6.res, 'N/A') as ACCURATE_VSLNAME,\n" +
				"  BL_NO,\n" +
				"  MASTER_BL_NO,\n" +
				"  I_E_MARK,\n" +
				"  BIZ_STAGE_NO,\n" +
				"  BIZ_STAGE_CODE,\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  uuid() as UUID,\n" +
				"  1 as BIZ_STATUS_IFFECTIVE\n" +
				"from commonTB\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',commonTB.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',commonTB.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=ds_out_door_status&TYPE_CODE=',commonTB.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table billTB_table = tEnv.sqlQuery("select * from billTB");
		tEnv.toAppendStream(billTB_table, Row.class).print();
//		env.execute();
		
		// TODO Oracle sink表，提单表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_track_biz_status_bill (\n" +
				"    VSL_IMO_NO STRING,\n" +
				"    VSL_NAME STRING,\n" +
				"    VOYAGE STRING,\n" +
				"    ACCURATE_IMONO STRING,\n" +
				"    ACCURATE_VSLNAME STRING,\n" +
				"    BL_NO STRING,\n" +
				"    MASTER_BL_NO STRING,\n" +
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
				"    PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO kafka sink表，提单
		tEnv.executeSql("" +
				"create table kafka_bill(\n" +
				"  GID STRING,\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING,\n" +
				"  DATA ROW(\n" +
				"    VSL_IMO_NO STRING,\n" +
				"    VSL_NAME STRING,\n" +
				"    VOYAGE STRING,\n" +
				"    ACCURATE_IMONO STRING,\n" +
				"    ACCURATE_VSLNAME STRING,\n" +
				"    BL_NO STRING,\n" +
				"    MASTER_BL_NO STRING,\n" +
				"    I_E_MARK STRING,\n" +
				"    BIZ_STAGE_NO STRING,\n" +
				"    BIZ_STAGE_CODE STRING,\n" +
				"    BIZ_STAGE_NAME STRING,\n" +
				"    BIZ_TIME TIMESTAMP(3),\n" +
				"    BIZ_STATUS_CODE STRING,\n" +
				"    BIZ_STATUS STRING,\n" +
				"    BIZ_STATUS_DESC STRING,\n" +
				"    LASTUPDATEDDT TIMESTAMP(3),\n" +
				"    ISDELETED int,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'T_CIQBMS_CIQDOR',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
				"  'format' = 'json'\n" +
				")");
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 写Oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"select\n" +
				"  billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
				"  billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,\n" +
				"  billTB.BIZ_STAGE_NAME,billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,\n" +
				"  billTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billTB.ISDELETED,billTB.UUID,billTB.BIZ_STATUS_IFFECTIVE\n" +
				"from billTB left join oracle_bill_dim FOR SYSTEM_TIME AS OF billTB.LASTUPDATEDDT as obd\n" +
				"  on billTB.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
				"  and billTB.VOYAGE = obd.VOYAGE\n" +
				"  and billTB.BL_NO = obd.BL_NO\n" +
				"  and billTB.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or billTB.BIZ_TIME>obd.BIZ_TIME)\n" +
				"  and billTB.BIZ_TIME is not null");
		
		// TODO 写Kafka
		statementSet.addInsertSql("" +
				"insert into kafka_bill\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_CIQDOR' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select\n" +
				"    billTB.UUID,billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
				"    billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,billTB.BIZ_STAGE_NAME,\n" +
				"    billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,billTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billTB.ISDELETED,billTB.BIZ_STATUS_IFFECTIVE\n" +
				"  from billTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF billTB.LASTUPDATEDDT as ospd2\n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_CIQDOR'=ospd2.APP_NAME\n" +
				"    and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
				"  where ospd2.ISCURRENT=1 and billTB.BIZ_TIME is not null\n" +
				"  ) as temp2");
		
		statementSet.execute();
	}
}
