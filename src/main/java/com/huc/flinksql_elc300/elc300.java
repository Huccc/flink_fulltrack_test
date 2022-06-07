package com.huc.flinksql_elc300;

import com.easipass.flink.table.function.udsf.JsonToRowInECL300;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class elc300 {
	public static void main(String[] args) throws Exception {
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
				"  'operate.type' = 'hash'\n" +
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
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass',\n" +
				"    'lookup.cache.max-rows' = '100',\n" +
				"    'lookup.cache.ttl' = '100',\n" +
				"    'lookup.max-retries' = '3'\n" +
				")");
		
		// TODO 注册临时函数
		tEnv.createTemporarySystemFunction("parseECL300", JsonToRowInECL300.class);
		
		// TODO 解析ECL300
		tEnv.executeSql("" +
				"create view sourceECL300TB as\n" +
				"select msgId,LASTUPDATEDDT,parseECL300(parseData).DataInfo.DockInfoList as pData,concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
				"from kafka_source_data where bizId='ECL300' and msgType='message_data'");
		
//		Table sourceECL300TB_table = tEnv.sqlQuery("select * from sourceECL300TB");
//		tEnv.toAppendStream(sourceECL300TB_table, Row.class).print();
//		env.execute();
		
		// TODO 展开数组
		tEnv.executeSql("" +
				"create view crossTB as\n" +
				"select msgId,LASTUPDATEDDT,DockInfo,GID\n" +
				"from (select * from sourceECL300TB where pData is not null) as tempTB1 cross join unnest(pData) AS pData(DockInfo)");
		
		// TODO 获取公共字段
		tEnv.executeSql("" +
				"create view commonTB as\n" +
				"select \n" +
				"  msgId,GID,\n" +
				"  if(DockInfo.SHIP_CODE <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(DockInfo.SHIP_CODE, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO,\n" +
				"  if(DockInfo.SHIP_NAME_EN <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.SHIP_NAME_EN, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"  if(DockInfo.VOYAGE <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.VOYAGE, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  if(DockInfo.BL_NO <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.BL_NO, '[\\t\\n\\r]', ''))), 'N/A') as BL_NO,\n" +
				"  if(DockInfo.CNTR_NO <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.CNTR_NO, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO,\n" +
				"  TO_TIMESTAMP(DockInfo.OPER_TIME, 'yyyyMMddHHmmss') as BIZ_TIME,\n" +
				"  'N/A' as MASTER_BL_NO,\n" +
				"  DockInfo.STATUS_CODE as BIZ_STATUS_CODE,\n" +
				"  'I' as I_E_MARK,\n" +
				"  'D7.7' as BIZ_STAGE_NO,\n" +
				"  'I_portOut_liftOff' as BIZ_STAGE_CODE,\n" +
				"  'N/A' as BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  0 as ISDELETED\n" +
				"from crossTB");

//		Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
//		tEnv.toAppendStream(commonTB_table, Row.class).print();
//		env.execute();
		
		// TODO 关联维表
		tEnv.executeSql("" +
				"create view withDIMTB as \n" +
				"select \n" +
				"  commonTB.*,\n" +
				"  if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
				"  if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务节点名称\n" +
				"  '已提离' as BIZ_STATUS, --业务状态\n" +
				"  uuid() as UUID,\n" +
				"  1 as BIZ_STATUS_IFFECTIVE\n" +
				"from commonTB\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim1 on concat('DIM:DIM_SHIP:IMO_NO=',commonTB.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim2 on concat('DIM:DIM_SHIP:IMO_NO=',commonTB.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim5 on concat('DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim6 on concat('DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim3 on concat('DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',commonTB.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',commonTB.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"--left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim4 on concat('DIM:DIM_COMMON_MINI:COMMON_CODE=lift_off_status&TYPE_CODE=',commonTB.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table withDIMTB_table = tEnv.sqlQuery("select * from withDIMTB");
		tEnv.toAppendStream(withDIMTB_table, Row.class).print();
		env.execute();
		
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
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO Kafka Sink 提单
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
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'elc300_bill_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'elc300_bill_test',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO Oracle Sink 箱
		tEnv.executeSql("" +
				"CREATE TABLE oracle_track_biz_status_ctnr (\n" +
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
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO Kafka Sink 箱
		tEnv.executeSql("" +
				"create table kafka_ctn(\n" +
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
				"    CTNR_NO STRING,\n" +
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
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'elc300_ctnr_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'elc300_ctnr_test',\n" +
				"  'format' = 'json'\n" +
				")");
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO Sink
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"select \n" +
				"  withDIMTB.VSL_IMO_NO,withDIMTB.VSL_NAME,withDIMTB.VOYAGE,withDIMTB.ACCURATE_IMONO,withDIMTB.ACCURATE_VSLNAME,\n" +
				"  withDIMTB.BL_NO,withDIMTB.MASTER_BL_NO,withDIMTB.I_E_MARK,withDIMTB.BIZ_STAGE_NO,withDIMTB.BIZ_STAGE_CODE,\n" +
				"  withDIMTB.BIZ_STAGE_NAME,withDIMTB.BIZ_TIME,withDIMTB.BIZ_STATUS_CODE,withDIMTB.BIZ_STATUS,\n" +
				"  withDIMTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,withDIMTB.ISDELETED,withDIMTB.UUID,withDIMTB.BIZ_STATUS_IFFECTIVE\n" +
				"from withDIMTB left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF withDIMTB.LASTUPDATEDDT as obd\n" +
				"on withDIMTB.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
				"and withDIMTB.VOYAGE = obd.VOYAGE\n" +
				"and withDIMTB.BL_NO = obd.BL_NO\n" +
				"and withDIMTB.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or withDIMTB.BIZ_TIME>obd.BIZ_TIME)\n" +
				"and withDIMTB.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_bill\n" +
				"select \n" +
				"  GID,'DATA_FLINK_FULL_FLINK_TRACING_LIFTOFF' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE, \n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select\n" +
				"  withDIMTB.GID,withDIMTB.VSL_IMO_NO,withDIMTB.VSL_NAME,withDIMTB.VOYAGE,withDIMTB.ACCURATE_IMONO,withDIMTB.ACCURATE_VSLNAME,\n" +
				"  withDIMTB.BL_NO,withDIMTB.MASTER_BL_NO,withDIMTB.I_E_MARK,withDIMTB.BIZ_STAGE_NO,withDIMTB.BIZ_STAGE_CODE,withDIMTB.BIZ_STAGE_NAME,\n" +
				"  withDIMTB.BIZ_TIME,withDIMTB.BIZ_STATUS_CODE,withDIMTB.BIZ_STATUS,withDIMTB.BIZ_STATUS_DESC,\n" +
				"  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,withDIMTB.ISDELETED,withDIMTB.UUID,withDIMTB.BIZ_STATUS_IFFECTIVE\n" +
				"from withDIMTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF withDIMTB.LASTUPDATEDDT as ospd2\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_LIFTOFF'=ospd2.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
				"where ospd2.ISCURRENT=1 and withDIMTB.BIZ_TIME is not null) as temp2");
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_ctnr \n" +
				"select \n" +
				"  withDIMTB.VSL_IMO_NO,withDIMTB.VSL_NAME,withDIMTB.VOYAGE,withDIMTB.ACCURATE_IMONO,withDIMTB.ACCURATE_VSLNAME,\n" +
				"  withDIMTB.CTNR_NO,withDIMTB.I_E_MARK,withDIMTB.BIZ_STAGE_NO,withDIMTB.BIZ_STAGE_CODE,withDIMTB.BIZ_STAGE_NAME,\n" +
				"  withDIMTB.BIZ_TIME,withDIMTB.BIZ_STATUS_CODE,withDIMTB.BIZ_STATUS,withDIMTB.BIZ_STATUS_DESC,\n" +
				"  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT, withDIMTB.ISDELETED, withDIMTB.UUID, withDIMTB.BIZ_STATUS_IFFECTIVE\n" +
				"from withDIMTB left join oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF withDIMTB.LASTUPDATEDDT as ocd\n" +
				"on withDIMTB.VSL_IMO_NO = ocd.VSL_IMO_NO\n" +
				"and withDIMTB.VOYAGE = ocd.VOYAGE\n" +
				"and withDIMTB.CTNR_NO = ocd.CTNR_NO\n" +
				"and withDIMTB.BIZ_STAGE_NO = ocd.BIZ_STAGE_NO\n" +
				"where (ocd.BIZ_TIME is null or withDIMTB.BIZ_TIME>ocd.BIZ_TIME)\n" +
				"and withDIMTB.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_ctn\n" +
				"select \n" +
				"  GID,'DATA_FLINK_FULL_FLINK_TRACING_LIFTOFF' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE, \n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from \n" +
				"(select \n" +
				"  withDIMTB.GID,withDIMTB.VSL_IMO_NO,withDIMTB.VSL_NAME,withDIMTB.VOYAGE,withDIMTB.ACCURATE_IMONO,withDIMTB.ACCURATE_VSLNAME,\n" +
				"  withDIMTB.CTNR_NO,withDIMTB.I_E_MARK,withDIMTB.BIZ_STAGE_NO,withDIMTB.BIZ_STAGE_CODE,withDIMTB.BIZ_STAGE_NAME,\n" +
				"  withDIMTB.BIZ_TIME,withDIMTB.BIZ_STATUS_CODE,withDIMTB.BIZ_STATUS,withDIMTB.BIZ_STATUS_DESC,\n" +
				"  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,withDIMTB.ISDELETED,withDIMTB.UUID,withDIMTB.BIZ_STATUS_IFFECTIVE\n" +
				"from withDIMTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF withDIMTB.LASTUPDATEDDT as ospd1\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_LIFTOFF'=ospd1.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd1.TABLE_NAME\n" +
				"where ospd1.ISCURRENT=1 and withDIMTB.BIZ_TIME is not null) as temp1");
		
		statementSet.execute();
	}
}
