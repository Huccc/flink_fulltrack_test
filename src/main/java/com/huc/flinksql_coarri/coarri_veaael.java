package com.huc.flinksql_coarri;

import com.easipass.flink.table.function.udsf.JsonArrayToRowInCOARRI;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class coarri_veaael {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO KAFKA数据源
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

//		Table kafka_source_data_table = tEnv.sqlQuery("select msgId from kafka_source_data");
//		tEnv.toAppendStream(kafka_source_data_table, Row.class).print();
//		env.execute();
		
		// TODO redis维表
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
				"  'redis.version' = '5.0'\n" +
				")");
		
		// TODO 创建船舶状态结果表(ORACLE)
		tEnv.executeSql("" +
				"create table track_biz_status_ship (\n" +
				"  VSL_IMO_NO String,\n" +
				"  VSL_NAME String,\n" +
				"  VOYAGE_IN String,\n" +
				"  VOYAGE_OUT String,\n" +
				"  CUS_CUSTOMS_CODE String,\n" +
				"  CUS_CUSTOMS_NAME String,\n" +
				"  MSA_DOCK_CODE String,\n" +
				"  MSA_DOCK_NAME String,\n" +
				"  MSA_BERTH_CODE String,\n" +
				"  MSA_BERTH_NAME String,\n" +
				"  DATA_SOURCE String,\n" +
				"  BIZ_STAGE_NO String,\n" +
				"  BIZ_STAGE_CODE String,\n" +
				"  BIZ_STAGE_NAME String,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE String,\n" +
				"  BIZ_STATUS String,\n" +
				"  BIZ_STATUS_DESC String,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE_IN,VOYAGE_OUT,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") with (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_SHIP',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO 创建船舶状态结果表（kafka）
		tEnv.executeSql("" +
				"create table kafka_ship(\n" +
				"  GID STRING,\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING,\n" +
				"  DATA ROW(\n" +
				"    VSL_IMO_NO String,\n" +
				"    VSL_NAME String,\n" +
				"    VOYAGE_IN String,\n" +
				"    VOYAGE_OUT String,\n" +
				"    CUS_CUSTOMS_CODE String,\n" +
				"    CUS_CUSTOMS_NAME String,\n" +
				"    MSA_DOCK_CODE String,\n" +
				"    MSA_DOCK_NAME String,\n" +
				"    MSA_BERTH_CODE String,\n" +
				"    MSA_BERTH_NAME String,\n" +
				"    DATA_SOURCE String,\n" +
				"    BIZ_STAGE_NO String,\n" +
				"    BIZ_STAGE_CODE String,\n" +
				"    BIZ_STAGE_NAME String,\n" +
				"    BIZ_TIME TIMESTAMP(3),\n" +
				"    BIZ_STATUS_CODE String,\n" +
				"    BIZ_STATUS String,\n" +
				"    BIZ_STATUS_DESC String,\n" +
				"    LASTUPDATEDDT TIMESTAMP(3),\n" +
				"    ISDELETED int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpevent-flink-push',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
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
		
		// TODO 创建船舶状态维表
		tEnv.executeSql("" +
				"create table track_biz_status_ship_dim (\n" +
				"  VSL_IMO_NO String,\n" +
				"  VSL_NAME String,\n" +
				"  VOYAGE_IN String,\n" +
				"  VOYAGE_OUT String,\n" +
				"  CUS_CUSTOMS_CODE String,\n" +
				"  CUS_CUSTOMS_NAME String,\n" +
				"  MSA_DOCK_CODE String,\n" +
				"  MSA_DOCK_NAME String,\n" +
				"  MSA_BERTH_CODE String,\n" +
				"  MSA_BERTH_NAME String,\n" +
				"  DATA_SOURCE String,\n" +
				"  BIZ_STAGE_NO String,\n" +
				"  BIZ_STAGE_CODE String,\n" +
				"  BIZ_STAGE_NAME String,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE String,\n" +
				"  BIZ_STATUS String,\n" +
				"  BIZ_STATUS_DESC String,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE_IN,VOYAGE_OUT,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") with (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_SHIP',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO 注册函数
		tEnv.createTemporarySystemFunction("JSON_STR_TO_ROW_IN_COARRI", JsonArrayToRowInCOARRI.class);
		
		// TODO 解析json串parseData
		tEnv.executeSql("" +
				"create view sourceview as\n" +
				"select\n" +
				"  msgId,bizId,msgType,bizUniqueId,destination,\n" +
				"  JSON_STR_TO_ROW_IN_COARRI(concat('{\"message\":',parseData,'}')).message as Pdata,\n" +
				"  LASTUPDATEDDT\n" +
				"from kafka_source_data where bizId='COARRI' and msgType='message_data'");
		
		// TODO 展开pData数组T_T
		tEnv.executeSql("" +
				"create view sourceTB as\n" +
				"select\n" +
				"  msgId,bizId,msgType,bizUniqueId,destination,VesselVoyageInformation,LASTUPDATEDDT\n" +
				"from (select * from sourceview where Pdata is not null) as tempTB1 cross join unnest(Pdata) AS Pdata(HeadRecord,VesselVoyageInformation,TallyCompanyInformation,ContainerInformation,DangerousGoods,RefrigeratedContainer,TailRecord)");

//		Table sourceTB_table = tEnv.sqlQuery("select * from sourceTB");
//		tEnv.toAppendStream(sourceTB_table, Row.class).print();
//		env.execute();
		
		// TODO 选择字段
		tEnv.executeSql("" +
				"create view commonTB as \n" +
				"select \n" +
				"  t1.*,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as VSL_IMO_NO\n" +
				"from \n" +
				"  (select \n" +
				"      msgId,\n" +
				"      VesselVoyageInformation.IEFlag as I_E_MARK,\n" +
				"      if(VesselVoyageInformation.VslName <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.VslName, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"      if(VesselVoyageInformation.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as voyage,  --航次\n" +
				"      'N/A' as CUS_CUSTOMS_CODE,\n" +
				"      'N/A' as CUS_CUSTOMS_NAME,\n" +
				"      'N/A' as MSA_DOCK_CODE,\n" +
				"      'N/A' as MSA_DOCK_NAME,\n" +
				"      'N/A' as MSA_BERTH_CODE,\n" +
				"      'N/A' as MSA_BERTH_NAME,\n" +
				"      'coarri' as DATA_SOURCE,\n" +
				"      TO_TIMESTAMP(VesselVoyageInformation.DchgStartTime,'yyyyMMddHHmm') as dischage_start_time,  --卸船开始时间\n" +
				"      TO_TIMESTAMP(VesselVoyageInformation.DchgCmplTime,'yyyyMMddHHmm') as discharge_completion_time,  --卸船结束时间\n" +
				"      TO_TIMESTAMP(VesselVoyageInformation.LoadStartTime,'yyyyMMddHHmm') as load_start_time,  --装船开始时间\n" +
				"      TO_TIMESTAMP(VesselVoyageInformation.LoadCmplTime,'yyyyMMddHHmm') as load_completion_time,  --装船结束时间\n" +
				"      '1' as BIZ_STATUS_CODE,  --业务状态代码\n" +
				"      'N/A' as BIZ_STATUS_DESC,  --业务状态详细描述\n" +
				"      LASTUPDATEDDT,  --最后处理时间\n" +
				"      0 as ISDELETED\n" +
				"  from sourceTB) as t1\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF t1.LASTUPDATEDDT as dim1 \n" +
				"  on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',t1.VSL_NAME)=dim1.key and 'IMO_NO'=dim1.hashkey");

//		Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
//		tEnv.toAppendStream(commonTB_table, Row.class).print();
//		env.execute();
		
		// TODO 进口
		tEnv.executeSql("" +
				"create view i_tb as \n" +
				"select \n" +
				"  t2.msgId,\n" +
				"  t2.VSL_IMO_NO,\n" +
				"  t2.VSL_NAME,\n" +
				"  t2.voyage as VOYAGE_IN,\n" +
				"  if(tbssd.VOYAGE_OUT <> '', tbssd.VOYAGE_OUT, 'N/A') as VOYAGE_OUT,\n" +
				"  t2.CUS_CUSTOMS_CODE,\n" +
				"  t2.CUS_CUSTOMS_NAME,\n" +
				"  t2.MSA_DOCK_CODE,\n" +
				"  t2.MSA_DOCK_NAME,\n" +
				"  t2.MSA_BERTH_CODE,\n" +
				"  t2.MSA_BERTH_NAME,\n" +
				"  t2.DATA_SOURCE,\n" +
				"  t2.dischage_start_time,\n" +
				"  t2.discharge_completion_time,\n" +
				"  t2.BIZ_STATUS_CODE,\n" +
				"  t2.BIZ_STATUS_DESC,\n" +
				"  t2.LASTUPDATEDDT,\n" +
				"  t2.ISDELETED\n" +
				"from (select * from commonTB where I_E_MARK='I') as t2\n" +
				"left join track_biz_status_ship_dim FOR SYSTEM_TIME as OF t2.LASTUPDATEDDT as tbssd\n" +
				"  on tbssd.VSL_NAME = t2.VSL_NAME --船名\n" +
				"  and tbssd.VOYAGE_IN = t2.voyage --航次");
		
		// TODO 出口
		tEnv.executeSql("" +
				"create view e_tb as \n" +
				"select \n" +
				"  t3.msgId,\n" +
				"  t3.VSL_IMO_NO,\n" +
				"  t3.VSL_NAME,\n" +
				"  if(tbssd1.VOYAGE_IN <> '', tbssd1.VOYAGE_IN, 'N/A') as VOYAGE_IN,\n" +
				"  t3.voyage as VOYAGE_OUT,\n" +
				"  t3.CUS_CUSTOMS_CODE,\n" +
				"  t3.CUS_CUSTOMS_NAME,\n" +
				"  t3.MSA_DOCK_CODE,\n" +
				"  t3.MSA_DOCK_NAME,\n" +
				"  t3.MSA_BERTH_CODE,\n" +
				"  t3.MSA_BERTH_NAME,\n" +
				"  t3.DATA_SOURCE,\n" +
				"  t3.load_start_time,\n" +
				"  t3.load_completion_time,\n" +
				"  t3.BIZ_STATUS_CODE,\n" +
				"  t3.BIZ_STATUS_DESC,\n" +
				"  t3.LASTUPDATEDDT,\n" +
				"  t3.ISDELETED\n" +
				"from (select * from commonTB where I_E_MARK='E') as t3\n" +
				"left join track_biz_status_ship_dim FOR SYSTEM_TIME as OF t3.LASTUPDATEDDT as tbssd1\n" +
				"  on tbssd1.VSL_NAME = t3.VSL_NAME --船名\n" +
				"  and tbssd1.VOYAGE_OUT = t3.voyage --航次");
		
		// TODO 总表
		tEnv.executeSql("" +
				"create view union_tb as\n" +
				"select\n" +
				"    msgId,VSL_IMO_NO,VSL_NAME,VOYAGE_IN,VOYAGE_OUT,\n" +
				"    CUS_CUSTOMS_CODE,CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"    MSA_DOCK_NAME,MSA_BERTH_CODE,MSA_BERTH_NAME,\n" +
				"    DATA_SOURCE,BIZ_STAGE_NO,BIZ_TIME,BIZ_STATUS_CODE,\n" +
				"    BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED\n" +
				"from\n" +
				"  (select \n" +
				"    msgId,VSL_IMO_NO,\n" +
				"    VSL_NAME,VOYAGE_IN,\n" +
				"    VOYAGE_OUT,CUS_CUSTOMS_CODE,\n" +
				"    CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"    MSA_DOCK_NAME,MSA_BERTH_CODE,\n" +
				"    MSA_BERTH_NAME,DATA_SOURCE,\n" +
				"    'D3.3t' as BIZ_STAGE_NO,\n" +
				"    dischage_start_time as BIZ_TIME,\n" +
				"    BIZ_STATUS_CODE,\n" +
				"    '开始卸船' as BIZ_STATUS,\n" +
				"    BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED\n" +
				"  from i_tb \n" +
				"  where dischage_start_time is not null)\n" +
				"union all\n" +
				"  (select \n" +
				"    msgId,VSL_IMO_NO,\n" +
				"    VSL_NAME,VOYAGE_IN,\n" +
				"    VOYAGE_OUT,CUS_CUSTOMS_CODE,\n" +
				"    CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"    MSA_DOCK_NAME,MSA_BERTH_CODE,\n" +
				"    MSA_BERTH_NAME,DATA_SOURCE,\n" +
				"    'D3.3c' as BIZ_STAGE_NO,\n" +
				"    discharge_completion_time as BIZ_TIME,\n" +
				"    BIZ_STATUS_CODE,\n" +
				"    '卸船结束' as BIZ_STATUS,\n" +
				"    BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED\n" +
				"  from i_tb \n" +
				"  where discharge_completion_time is not null)\n" +
				"union all\n" +
				"  (select \n" +
				"    msgId,VSL_IMO_NO,\n" +
				"    VSL_NAME,VOYAGE_IN,\n" +
				"    VOYAGE_OUT,CUS_CUSTOMS_CODE,\n" +
				"    CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"    MSA_DOCK_NAME,MSA_BERTH_CODE,\n" +
				"    MSA_BERTH_NAME,DATA_SOURCE,\n" +
				"    'C8.4t' as BIZ_STAGE_NO,\n" +
				"    load_start_time as BIZ_TIME,\n" +
				"    BIZ_STATUS_CODE,\n" +
				"    '开始装船' as BIZ_STATUS,\n" +
				"    BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED\n" +
				"  from e_tb \n" +
				"  where load_start_time is not null)\n" +
				"union all\n" +
				"  (select \n" +
				"    msgId,VSL_IMO_NO,\n" +
				"    VSL_NAME,VOYAGE_IN,\n" +
				"    VOYAGE_OUT,CUS_CUSTOMS_CODE,\n" +
				"    CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"    MSA_DOCK_NAME,MSA_BERTH_CODE,\n" +
				"    MSA_BERTH_NAME,DATA_SOURCE,\n" +
				"    'C8.4c' as BIZ_STAGE_NO,\n" +
				"    load_completion_time as BIZ_TIME,\n" +
				"    BIZ_STATUS_CODE,\n" +
				"    '装船结束' as BIZ_STATUS,\n" +
				"    BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED\n" +
				"  from e_tb \n" +
				"  where load_completion_time is not null)");
		
		// TODO 关联redis，获取业务环节节点代码、业务环节节点名称
		tEnv.executeSql("" +
				"create view res_tb as\n" +
				"select \n" +
				"  msgId,VSL_IMO_NO,VSL_NAME,VOYAGE_IN,VOYAGE_OUT,CUS_CUSTOMS_CODE,\n" +
				"  CUS_CUSTOMS_NAME,MSA_DOCK_CODE,MSA_DOCK_NAME,MSA_BERTH_CODE,\n" +
				"  MSA_BERTH_NAME,DATA_SOURCE,BIZ_STAGE_NO,\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as BIZ_STAGE_CODE,\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,ISDELETED\n" +
				"from union_tb\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF union_tb.LASTUPDATEDDT as dim2 \n" +
				"  on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',union_tb.BIZ_STAGE_NO) = dim2.key and 'SUB_STAGE_CODE' = dim2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF union_tb.LASTUPDATEDDT as dim3 \n" +
				"  on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',union_tb.BIZ_STAGE_NO) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey");
		
		Table res_tb_table = tEnv.sqlQuery("select * from res_tb");
		tEnv.toAppendStream(res_tb_table, Row.class).print();
		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 写到Oracle
		statementSet.addInsertSql("" +
				"insert into track_biz_status_ship (VSL_IMO_NO,VSL_NAME,VOYAGE_IN,VOYAGE_OUT,CUS_CUSTOMS_CODE,CUS_CUSTOMS_NAME,MSA_DOCK_CODE,MSA_DOCK_NAME,MSA_BERTH_CODE,MSA_BERTH_NAME,DATA_SOURCE,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED)\n" +
				"select \n" +
				"  VSL_IMO_NO,VSL_NAME,VOYAGE_IN,VOYAGE_OUT,\n" +
				"  CUS_CUSTOMS_CODE,CUS_CUSTOMS_NAME,MSA_DOCK_CODE,\n" +
				"  MSA_DOCK_NAME,MSA_BERTH_CODE,MSA_BERTH_NAME,\n" +
				"  DATA_SOURCE,BIZ_STAGE_NO,BIZ_STAGE_CODE,\n" +
				"  BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,\n" +
				"  BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED\n" +
				"from res_tb");
		
		// TODO 写到kafka
		statementSet.addInsertSql("" +
				"insert into kafka_ship\n" +
				"select \n" +
				"  msgId as GID,'DATA_FLINK_FULL_FLINK_TRACING_COARRIVESSEL' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_SHIP' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE, \n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE_IN,VOYAGE_OUT,CUS_CUSTOMS_CODE,CUS_CUSTOMS_NAME,MSA_DOCK_CODE,MSA_DOCK_NAME,MSA_BERTH_CODE,MSA_BERTH_NAME,DATA_SOURCE,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED) as DATA\n" +
				"from \n" +
				"(select msgId,\n" +
				"  res_tb.VSL_IMO_NO,res_tb.VSL_NAME,res_tb.VOYAGE_IN,res_tb.VOYAGE_OUT,\n" +
				"  res_tb.CUS_CUSTOMS_CODE,res_tb.CUS_CUSTOMS_NAME,res_tb.MSA_DOCK_CODE,\n" +
				"  res_tb.MSA_DOCK_NAME,res_tb.MSA_BERTH_CODE,res_tb.MSA_BERTH_NAME,\n" +
				"  res_tb.DATA_SOURCE,res_tb.BIZ_STAGE_NO,res_tb.BIZ_STAGE_CODE,\n" +
				"  res_tb.BIZ_STAGE_NAME,res_tb.BIZ_TIME,res_tb.BIZ_STATUS_CODE,\n" +
				"  res_tb.BIZ_STATUS,res_tb.BIZ_STATUS_DESC,res_tb.LASTUPDATEDDT,res_tb.ISDELETED\n" +
				"from res_tb left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF res_tb.LASTUPDATEDDT as ospd2\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_COARRIVESSEL'=ospd2.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_SHIP'=ospd2.TABLE_NAME\n" +
				"where ospd2.ISCURRENT=1) as t4");
		
		statementSet.execute();
	}
}
