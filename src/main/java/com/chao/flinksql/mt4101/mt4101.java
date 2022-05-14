package com.chao.flinksql.mt4101;

import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class mt4101 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
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
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'group-flink-msg-mt4101',\n" +
				"  'scan.startup.mode' = 'group-offsets',\n" +
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
				"  'operate.type' = 'hash',\n" +
				"  'redis.version' = '5.0'\n" +
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
				"  --'dialect-name' = 'Oracle',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
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
				"  --'dialect-name' = 'Oracle',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass',\n" +
				"  'lookup.cache.max-rows' = '100',\n" +
				"  'lookup.cache.ttl' = '100000',\n" +
				"  'lookup.max-retries' = '3'\n" +
				")");
		
		// TODO 注册9999解析函数
		tEnv.createTemporarySystemFunction("parseMT4101fromMT9999", JsonToRowInMt9999.class);
		
		// TODO 解析mt9999
		tEnv.executeSql("create view source9999TB as\n" +
				"select\n" +
				"  msgId, LASTUPDATEDDT,\n" +
				"  parseMT4101fromMT9999(parseData) as pData\n" +
				"from kafka_source_data where bizId='MT9999' and msgType='message_data'");
		
		Table source9999TB_table = tEnv.sqlQuery("select * from source9999TB");
		tEnv.toAppendStream(source9999TB_table, Row.class).print();
//		env.execute();
		
		// TODO 9999获取报文公共字段
		tEnv.executeSql("" +
				"create view common9999TB as\n" +
				"select\n" +
				"  msgId,LASTUPDATEDDT,\n" +
				"  pData.Head.MessageID as Head_MessageID,\n" +
				"  pData.Head.MessageType as MessageType,\n" +
				"  if(REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN','') <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO,\n" +
				"  if(UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))) <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  TO_TIMESTAMP(concat(substr(pData.Head.SendTime, 1, 14), '.', substr(pData.Head.SendTime, 15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME,\n" +
				"  if(pData.Head.FunctionCode='3', 1, 0) as ISDELETED,\n" +
				"  pData.Response.Consignment as Consignment\n" +
				"from source9999TB where MessageType='MT4101'");
		
		Table common9999TB_table = tEnv.sqlQuery("select * from common9999TB");
		tEnv.toAppendStream(common9999TB_table, Row.class).print();
		env.execute();
		
		// TODO 展开9999提单
		tEnv.executeSql("" +
				"create view mt9999withBill as\n" +
				"select\n" +
				"  msgId, LASTUPDATEDDT, Head_MessageID, VSL_IMO_NO, VOYAGE, BIZ_TIME, ISDELETED, --'I' as I_E_MARK,\n" +
				"  if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as BL_NO,\n" +
				"  'N/A' as MASTER_BL_NO,\n" +
				"  ResponseType.Code as BIZ_STATUS_CODE,\n" +
				"  if(ResponseType.Text <> '', ResponseType.Text, 'N/A') as BIZ_STATUS_DESC\n" +
				"from (select * from common9999TB where Consignment is not null) as tempTB2\n" +
				"cross join unnest(Consignment) AS Consignment(TransportContractDocument,AssociatedTransportDocument,ResponseType)\n" +
//				"where ResponseType.Code='01'" +
				"");
		
		Table mt9999withBill_table = tEnv.sqlQuery("select * from mt9999withBill");
		tEnv.toAppendStream(mt9999withBill_table, Row.class).print();
//		env.execute();
		
		// TODO 提单结果表
		tEnv.executeSql("" +
				"create view resBill as\n" +
				"select\n" +
				"  msgId,  VSL_IMO_NO,\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as VSL_NAME,\n" +
				"  VOYAGE,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_IMONO,\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_VSLNAME,\n" +
				"  BL_NO,\n" +
				"  MASTER_BL_NO,\n" +
				"  'E' as I_E_MARK,\n" +
				"  'C8.5' as BIZ_STAGE_NO,\n" +
				"  'E_cusDecl_MT4101' as BIZ_STAGE_CODE,\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  uuid() as UUID,\n" +
				"  if(BIZ_STATUS_CODE='01' and BIZ_STATUS_DESC not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from mt9999withBill\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999withBill.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999withBill.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999withBill.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999withBill.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999withBill.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=C8.5&SUB_STAGE_CODE=E_cusDecl_MT4101') = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999withBill.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999withBill.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table resBill_table = tEnv.sqlQuery("select * from resBill");
		tEnv.toAppendStream(resBill_table, Row.class).print();
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
				"  --'dialect-name' = 'Oracle',\n" +
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
				"      VSL_IMO_NO STRING,\n" +
				"      VSL_NAME STRING,\n" +
				"      VOYAGE STRING,\n" +
				"      ACCURATE_IMONO STRING,\n" +
				"      ACCURATE_VSLNAME STRING,\n" +
				"      BL_NO STRING,\n" +
				"      MASTER_BL_NO STRING,\n" +
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
				"      BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpevent-flink-push',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 写入Oracle结果表
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"select\n" +
				"  resBill.VSL_IMO_NO,resBill.VSL_NAME,resBill.VOYAGE,resBill.ACCURATE_IMONO,resBill.ACCURATE_VSLNAME,\n" +
				"  resBill.BL_NO,resBill.MASTER_BL_NO,resBill.I_E_MARK,resBill.BIZ_STAGE_NO,resBill.BIZ_STAGE_CODE,\n" +
				"  resBill.BIZ_STAGE_NAME,resBill.BIZ_TIME,resBill.BIZ_STATUS_CODE,resBill.BIZ_STATUS,\n" +
				"  resBill.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,resBill.ISDELETED,\n" +
				"  resBill.UUID,resBill.BIZ_STATUS_IFFECTIVE\n" +
				"from resBill left join oracle_bill_dim FOR SYSTEM_TIME AS OF resBill.LASTUPDATEDDT as obd\n" +
				"  on resBill.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
				"  and resBill.VOYAGE = obd.VOYAGE\n" +
				"  and resBill.BL_NO = obd.BL_NO\n" +
				"  and resBill.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or resBill.BIZ_TIME>obd.BIZ_TIME)\n" +
				"  and resBill.BIZ_TIME is not null");
		
		// TODO 写入Kafka结果表
		statementSet.addInsertSql("" +
				"insert into kafka_bill\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT4101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select\n" +
				"      resBill.UUID,resBill.VSL_IMO_NO,resBill.VSL_NAME,resBill.VOYAGE,resBill.ACCURATE_IMONO,resBill.ACCURATE_VSLNAME,\n" +
				"      resBill.BL_NO,resBill.MASTER_BL_NO,resBill.I_E_MARK,resBill.BIZ_STAGE_NO,resBill.BIZ_STAGE_CODE,resBill.BIZ_STAGE_NAME,\n" +
				"      resBill.BIZ_TIME,resBill.BIZ_STATUS_CODE,resBill.BIZ_STATUS,resBill.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,resBill.ISDELETED,resBill.BIZ_STATUS_IFFECTIVE\n" +
				"  from resBill left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF resBill.LASTUPDATEDDT as ospd2\n" +
				"      on 'DATA_FLINK_FULL_FLINK_TRACING_MT4101'=ospd2.APP_NAME\n" +
				"      and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
				"  where ospd2.ISCURRENT=1 and resBill.BIZ_TIME is not null\n" +
				"  ) as temp2");
		
		statementSet.execute();
	}
}
