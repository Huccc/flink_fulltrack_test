package com.chao.flinksql.coarri.coarri_container;

import com.easipass.flink.table.function.udsf.JsonArrayToRowInCOARRI;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class coarri_container {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		// TODO KAFKA 数据源
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
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-coarri-container',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'latest-offset'\n" +
				")");

//		Table kafka_source_data_table = tEnv.sqlQuery("select * from kafka_source_data");
//		tEnv.toAppendStream(kafka_source_data_table, Row.class).print();
//		env.execute();
		
		// TODO DIM redis维度表
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
		
		// TODO ORACLE 提单 dim 维度表
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
		
		// TODO ORACLE 箱 dim 维度表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_ctnr_dim (\n" +
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
		
		// TODO 是否推送kafka配置表
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
				"  'lookup.cache.ttl' = '20000',\n" +
				"  'lookup.max-retries' = '3'\n" +
				")");
		
		// TODO ORACLE 提单状态表
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
		
		// TODO ORACLE 箱表
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
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO ORACLE箱单关系表 dim
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
		
		// TODO KAFKA 提单表
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
				"  'topic' = 'topic-bdpevent-flink-push',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO KAFKA 箱表
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
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpevent-flink-push',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
				"  'format' = 'json'\n" +
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

//		Table sourceview_table = tEnv.sqlQuery("select * from sourceview");
//		tEnv.toAppendStream(sourceview_table, Row.class).print();
//		env.execute();
		
		// TODO 展开pData数组T_T
		tEnv.executeSql("" +
				"create view sourceTB as\n" +
				"select\n" +
				"  msgId,bizId,msgType,bizUniqueId,destination,VesselVoyageInformation,HeadRecord,ContainerInformation,LASTUPDATEDDT\n" +
				"from (select * from sourceview where Pdata is not null) as tempTB1\n" +
				"cross join unnest(Pdata) AS Pdata(HeadRecord,VesselVoyageInformation,TallyCompanyInformation,ContainerInformation,DangerousGoods,RefrigeratedContainer,TailRecord)");

//		Table sourceTB_table = tEnv.sqlQuery("select * from sourceTB");
//		tEnv.toAppendStream(sourceTB_table, Row.class).print();
//		env.execute();
		
		// TODO 提单和箱的公共字段
		tEnv.executeSql("" +
				"create view commnontb as\n" +
				"select temp1.*,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as VSL_IMO_NO,--在这个逻辑中标准IMO和船舶IMO一样，因为船舶IMO在平文中没有\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_IMONO,\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_VSLNAME,\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS\n" +
				"from\n" +
				"  (select\n" +
				"    --提单和箱的公共字段\n" +
				"    msgId,--用来推送到kafka\n" +
				"    if(VesselVoyageInformation.VslName <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.VslName, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"    if(VesselVoyageInformation.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"    'N/A' as MASTER_BL_NO,\n" +
				"    VesselVoyageInformation.IEFlag as I_E_MARK,\n" +
				"    if(VesselVoyageInformation.IEFlag = 'E', 'C8.4', 'D3.3') as BIZ_STAGE_NO,\n" +
				"    if(VesselVoyageInformation.IEFlag = 'E', 'E_vslLoading_coarri', 'I_vslUnloading_coarri') as BIZ_STAGE_CODE,\n" +
				"    '1' as BIZ_STATUS_CODE,\n" +
				"    if(HeadRecord.FileDesp is not null, HeadRecord.FileDesp, 'N/A') as BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,\n" +
				"    0 as ISDELETED,\n" +
				"    ContainerInformation --箱\n" +
				"  from sourceTB where HeadRecord.FileDesp <> 'SBARGE') as temp1\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp1.VSL_NAME)=dim1.key and 'IMO_NO'=dim1.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp1.VSL_NAME)=dim2.key and 'VSL_NAME_EN'=dim2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',temp1.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',temp1.BIZ_STAGE_CODE)=dim3.key and 'SUB_STAGE_NAME'=dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=',if(temp1.I_E_MARK ='E', 'load_status', 'discharge_status'),'&TYPE_CODE=',temp1.BIZ_STATUS_CODE)=dim4.key and 'TYPE_NAME'=dim4.hashkey");

//		Table commnontb_table = tEnv.sqlQuery("select * from commnontb");
//		tEnv.toAppendStream(commnontb_table, Row.class).print();
//		env.execute();
		
		// TODO 展开箱，获取箱状态表
		tEnv.executeSql("" +
				"create view ctnrTB as\n" +
				"select\n" +
				"  msgId,--用来推送到kafka\n" +
				"  VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"  if(CtnrNo <> '', UPPER(TRIM(REGEXP_REPLACE(CtnrNo, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO,\n" +
				"  I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"  TO_TIMESTAMP(ActualLoadDchgTime,'yyyyMMddHHmm') as BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,\n" +
				"  BillOfLadingInformation,MASTER_BL_NO, --提单\n" +
				"  uuid() as UUID,1 as BIZ_STATUS_IFFECTIVE " +
				"from (select * from commnontb where ContainerInformation is not null) as temp2\n" +
				"CROSS JOIN UNNEST(ContainerInformation) AS ContainerInformation(RecId,CtnrNo,CtnrSizeType,CtnrOprCode,CtnrOprName,CntrStatusCode,SealNo,BayNo,CargoNetWtInCtnr,PkgQtyInCtnr,CargoVolumeInCtnr,TeuQty,ActualLoadDchgTime,IntlTsMark,OverLengthFront,OverLengthBack,OverWidthRight,OverWidthLeft,OverHeight,LocationInformation,BillOfLadingInformation)");

		Table ctnrTB_table = tEnv.sqlQuery("select * from ctnrTB");
		tEnv.toAppendStream(ctnrTB_table, Row.class).print();
		env.execute();
		
		// TODO 展开箱中的提单，获取提单号
		tEnv.executeSql("" +
				"create view billFromCtnr as\n" +
				"select\n" +
				"  UUID,--用来推送到kafka\n" +
				"  BIZ_STATUS_IFFECTIVE,\n" +
				"  VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"  if(BlNo <> '', UPPER(TRIM(REGEXP_REPLACE(BlNo, '[\\t\\n\\r]', ''))), 'N/A') as BL_NO,--提单号\n" +
				"  MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,\n" +
				"  BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED\n" +
				"from (select * from ctnrTB where BillOfLadingInformation is not null) as temp3\n" +
				"CROSS JOIN UNNEST(BillOfLadingInformation) as BillOfLadingInformation(RecId,BlNo,LoadPortCode,DchgPortCode,TsMark,PkgQty,CargoGrossWt,GrossVolumn,CargoDesp)");

//		Table billFromCtnr_table = tEnv.sqlQuery("select * from billFromCtnr");
//		tEnv.toAppendStream(billFromCtnr_table, Row.class).print();
//		env.execute();
		
		// TODO 关联箱单关系表取分提单号
		tEnv.executeSql("" +
				"create view billwithblctnr as\n" +
				"select\n" +
				"  billFromCtnr.UUID,billFromCtnr.BIZ_STATUS_IFFECTIVE,billFromCtnr.VSL_IMO_NO,billFromCtnr.VSL_NAME,billFromCtnr.VOYAGE,billFromCtnr.ACCURATE_IMONO,billFromCtnr.ACCURATE_VSLNAME,\n" +
				"  obcd.BL_NO,obcd.MASTER_BL_NO,billFromCtnr.I_E_MARK,billFromCtnr.BIZ_STAGE_NO,billFromCtnr.BIZ_STAGE_CODE,\n" +
				"  billFromCtnr.BIZ_STAGE_NAME,billFromCtnr.BIZ_TIME,billFromCtnr.BIZ_STATUS_CODE,billFromCtnr.BIZ_STATUS,\n" +
				"  billFromCtnr.BIZ_STATUS_DESC,billFromCtnr.LASTUPDATEDDT,billFromCtnr.ISDELETED\n" +
				"from billFromCtnr left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF billFromCtnr.LASTUPDATEDDT as obcd  --拿分提单号\n" +
				"  on billFromCtnr.VSL_NAME=obcd.VSL_NAME\n" +
				"  and billFromCtnr.VOYAGE=obcd.VOYAGE\n" +
				"  and billFromCtnr.BL_NO=obcd.MASTER_BL_NO\n" +
				"where obcd.BL_NO<>'N/A' and obcd.BL_NO is not null  --过滤掉不是分提单的情况\n" +
				"  and obcd.MASTER_BL_NO<>'N/A'");

		Table billwithblctnr_table = tEnv.sqlQuery("select * from billwithblctnr");
		tEnv.toAppendStream(billwithblctnr_table,Row.class).print();
		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 写入Oracle箱状态表
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_ctnr(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  ctnrTB.VSL_IMO_NO,ctnrTB.VSL_NAME,ctnrTB.VOYAGE,ctnrTB.ACCURATE_IMONO,ctnrTB.ACCURATE_VSLNAME,\n" +
				"  ctnrTB.CTNR_NO,ctnrTB.I_E_MARK,ctnrTB.BIZ_STAGE_NO,ctnrTB.BIZ_STAGE_CODE,ctnrTB.BIZ_STAGE_NAME,\n" +
				"  ctnrTB.BIZ_TIME,ctnrTB.BIZ_STATUS_CODE,ctnrTB.BIZ_STATUS,ctnrTB.BIZ_STATUS_DESC,\n" +
				"  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,ctnrTB.ISDELETED,\n" +
				"  ctnrTB.UUID,ctnrTB.BIZ_STATUS_IFFECTIVE\n" +
				"from ctnrTB left join oracle_ctnr_dim FOR SYSTEM_TIME as OF ctnrTB.LASTUPDATEDDT as ocd\n" +
				"  on ctnrTB.VSL_IMO_NO=ocd.VSL_IMO_NO\n" +
				"  and ctnrTB.VOYAGE=ocd.VOYAGE\n" +
				"  and ctnrTB.CTNR_NO=ocd.CTNR_NO\n" +
				"  and ctnrTB.BIZ_STAGE_NO=ocd.BIZ_STAGE_NO\n" +
				"where (ocd.BIZ_TIME is null or ctnrTB.BIZ_TIME>ocd.BIZ_TIME) --前者插入后者更新\n" +
				"  and ctnrTB.BIZ_TIME is not null");
		
		// TODO 写入kafka箱状态表
		statementSet.addInsertSql("" +
				"insert into kafka_ctn (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"   GID, 'DATA_FLINK_FULL_FLINK_TRACING_COARRI' as APP_NAME, 'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"   ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select\n" +
				"    ct1.UUID as GID,ct1.VSL_IMO_NO,ct1.VSL_NAME,ct1.VOYAGE,\n" +
				"    ct1.ACCURATE_IMONO,ct1.ACCURATE_VSLNAME,ct1.CTNR_NO,\n" +
				"    ct1.I_E_MARK,ct1.BIZ_STAGE_NO,ct1.BIZ_STAGE_CODE,\n" +
				"    ct1.BIZ_STAGE_NAME,ct1.BIZ_TIME,ct1.BIZ_STATUS_CODE,\n" +
				"    ct1.BIZ_STATUS,ct1.BIZ_STATUS_DESC,\n" +
				"    cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,ct1.ISDELETED,ct1.BIZ_STATUS_IFFECTIVE\n" +
				"  from ctnrTB as ct1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF ct1.LASTUPDATEDDT as ospd\n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_COARRI'=ospd.APP_NAME\n" +
				"    AND 'DM.TRACK_BIZ_STATUS_CTNR'=ospd.TABLE_NAME\n" +
				"  where ospd.ISCURRENT=1 and ct1.BIZ_TIME is not null\n" +
				"  ) as ct2");
		
		// TODO 写入Oracle提单状态表，对于没有分提单的
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  billFromCtnr.VSL_IMO_NO,billFromCtnr.VSL_NAME,billFromCtnr.VOYAGE,billFromCtnr.ACCURATE_IMONO,billFromCtnr.ACCURATE_VSLNAME,\n" +
				"  billFromCtnr.BL_NO,billFromCtnr.MASTER_BL_NO,billFromCtnr.I_E_MARK,billFromCtnr.BIZ_STAGE_NO,billFromCtnr.BIZ_STAGE_CODE,\n" +
				"  billFromCtnr.BIZ_STAGE_NAME,billFromCtnr.BIZ_TIME,billFromCtnr.BIZ_STATUS_CODE,billFromCtnr.BIZ_STATUS,\n" +
				"  billFromCtnr.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billFromCtnr.ISDELETED,billFromCtnr.UUID,billFromCtnr.BIZ_STATUS_IFFECTIVE\n" +
				"from billFromCtnr left join oracle_bill_dim FOR SYSTEM_TIME as OF billFromCtnr.LASTUPDATEDDT as obd\n" +
				"  on billFromCtnr.VSL_IMO_NO=obd.VSL_IMO_NO\n" +
				"  and billFromCtnr.VOYAGE=obd.VOYAGE\n" +
				"  and billFromCtnr.BL_NO=obd.BL_NO\n" +
				"  and billFromCtnr.BIZ_STAGE_NO=obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or billFromCtnr.BIZ_TIME>obd.BIZ_TIME) --前者插入后者更新\n" +
				"  and billFromCtnr.BIZ_TIME is not null");
		
		// TODO 写入kafka提单状态表
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  GID, 'DATA_FLINK_FULL_FLINK_TRACING_COARRI' as APP_NAME, 'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select\n" +
				"    bc1.UUID as GID,bc1.VSL_IMO_NO,bc1.VSL_NAME,bc1.VOYAGE,\n" +
				"    bc1.ACCURATE_IMONO,bc1.ACCURATE_VSLNAME,bc1.BL_NO,\n" +
				"    bc1.MASTER_BL_NO,bc1.I_E_MARK,bc1.BIZ_STAGE_NO,\n" +
				"    bc1.BIZ_STAGE_CODE,bc1.BIZ_STAGE_NAME,bc1.BIZ_TIME,\n" +
				"    bc1.BIZ_STATUS_CODE,bc1.BIZ_STATUS,bc1.BIZ_STATUS_DESC,\n" +
				"    cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,bc1.ISDELETED,\n" +
				"    bc1.BIZ_STATUS_IFFECTIVE\n" +
				"  from billFromCtnr as bc1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bc1.LASTUPDATEDDT as ospd\n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_COARRI'=ospd.APP_NAME\n" +
				"    AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd.TABLE_NAME\n" +
				"  where ospd.ISCURRENT=1 and bc1.BIZ_TIME is not null\n" +
				"  ) as bc2");
		
		// TODO 从箱单关系表取分提单号，写Oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  billwithblctnr.VSL_IMO_NO,billwithblctnr.VSL_NAME,billwithblctnr.VOYAGE,billwithblctnr.ACCURATE_IMONO,billwithblctnr.ACCURATE_VSLNAME,\n" +
				"  billwithblctnr.BL_NO,billwithblctnr.MASTER_BL_NO,billwithblctnr.I_E_MARK,billwithblctnr.BIZ_STAGE_NO,billwithblctnr.BIZ_STAGE_CODE,\n" +
				"  billwithblctnr.BIZ_STAGE_NAME,billwithblctnr.BIZ_TIME,billwithblctnr.BIZ_STATUS_CODE,billwithblctnr.BIZ_STATUS,\n" +
				"  billwithblctnr.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billwithblctnr.ISDELETED,billwithblctnr.UUID,billwithblctnr.BIZ_STATUS_IFFECTIVE\n" +
				"from billwithblctnr left join oracle_bill_dim FOR SYSTEM_TIME AS OF billwithblctnr.LASTUPDATEDDT as obd1  --判断新旧插入或更新\n" +
				"  on billwithblctnr.VSL_IMO_NO=obd1.VSL_IMO_NO\n" +
				"  and billwithblctnr.VOYAGE=obd1.VOYAGE\n" +
				"  and billwithblctnr.BL_NO=obd1.BL_NO\n" +
				"  and billwithblctnr.BIZ_STAGE_NO=obd1.BIZ_STAGE_NO\n" +
				"where (obd1.BIZ_TIME is null or billwithblctnr.BIZ_TIME>obd1.BIZ_TIME) --前者插入后者更新\n" +
				"  and billwithblctnr.BIZ_TIME is not null");
		
		// TODO 写入kafka提单状态表
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  GID, 'DATA_FLINK_FULL_FLINK_TRACING_COARRI' as APP_NAME, 'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select\n" +
				"    bc3.UUID as GID,bc3.VSL_IMO_NO,bc3.VSL_NAME,bc3.VOYAGE,\n" +
				"    bc3.ACCURATE_IMONO,bc3.ACCURATE_VSLNAME,bc3.BL_NO,\n" +
				"    bc3.MASTER_BL_NO,bc3.I_E_MARK,bc3.BIZ_STAGE_NO,\n" +
				"    bc3.BIZ_STAGE_CODE,bc3.BIZ_STAGE_NAME,bc3.BIZ_TIME,\n" +
				"    bc3.BIZ_STATUS_CODE,bc3.BIZ_STATUS,bc3.BIZ_STATUS_DESC,\n" +
				"    cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,bc3.ISDELETED,bc3.BIZ_STATUS_IFFECTIVE\n" +
				"  from billwithblctnr as bc3 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bc3.LASTUPDATEDDT as ospd1\n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_COARRI'=ospd1.APP_NAME\n" +
				"    AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd1.TABLE_NAME\n" +
				"  where ospd1.ISCURRENT=1 and bc3.BIZ_TIME is not null\n" +
				"  ) as bc4");
		
		statementSet.execute();
	}
}
