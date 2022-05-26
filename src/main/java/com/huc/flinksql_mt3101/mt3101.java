package com.huc.flinksql_mt3101;

import com.easipass.flink.table.function.udsf.JsonToRowInMt3101;
import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class mt3101 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE DATA_XPQ_MSG_PARSE_RESULT (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'mt3101',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
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
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_bill_dim (\n" +
				"  VSL_IMO_NO STRING,\n" +
				"  VSL_NAME STRING,\n" +
				"  VOYAGE STRING,\n" +
				"  ACCURATE_IMONO STRING,\n" +
				"  ACCURATE_VSLNAME STRING,\n" +
				"  BL_NO STRING,\n" +
				"  MASTER_BL_NO STRING,\n" +
				"  I_E_MARK STRING,\n" +
				"  BIZ_STAGE_NO STRING,\n" +
				"  BIZ_STAGE_CODE STRING,\n" +
				"  BIZ_STAGE_NAME STRING,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE STRING,\n" +
				"  BIZ_STATUS STRING,\n" +
				"  BIZ_STATUS_DESC STRING,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  UUID STRING,\n" +
				"  BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO,BIZ_STATUS_CODE) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_ctnr_dim (\n" +
				"  VSL_IMO_NO STRING,\n" +
				"  VSL_NAME STRING,\n" +
				"  VOYAGE STRING,\n" +
				"  ACCURATE_IMONO STRING,\n" +
				"  ACCURATE_VSLNAME STRING,\n" +
				"  CTNR_NO STRING,\n" +
				"  I_E_MARK STRING,\n" +
				"  BIZ_STAGE_NO STRING,\n" +
				"  BIZ_STAGE_CODE STRING,\n" +
				"  BIZ_STAGE_NAME STRING,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE STRING,\n" +
				"  BIZ_STATUS STRING,\n" +
				"  BIZ_STATUS_DESC STRING,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  UUID STRING,\n" +
				"  BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO,BIZ_STATUS_CODE) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_blctnr_dim (\n" +
				"  VSL_IMO_NO STRING,\n" +
				"  VSL_NAME STRING,\n" +
				"  VOYAGE STRING,\n" +
				"  ACCURATE_IMONO STRING,\n" +
				"  ACCURATE_VSLNAME STRING,\n" +
				"  BL_NO STRING,\n" +
				"  MASTER_BL_NO STRING,\n" +
				"  CTNR_NO STRING,\n" +
				"  I_E_MARK STRING,\n" +
				"  MESSAGE_ID STRING,\n" +
				"  SENDER_CODE STRING,\n" +
				"  BULK_FLAG STRING,\n" +
				"  RSP_CREATE_TIME TIMESTAMP,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,CTNR_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_BLCTNR',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_subscribe_papam_dim (\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING,\n" +
				"  SUBSCRIBER STRING,\n" +
				"  DB_URL STRING,\n" +
				"  KAFKA_SERVERS STRING,\n" +
				"  KAFKA_SINK_TOPIC STRING,\n" +
				"  ISCURRENT DECIMAL(11, 0),\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  PRIMARY KEY(APP_NAME,TABLE_NAME) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass',\n" +
				"  'lookup.cache.max-rows' = '100',\n" +
				"  'lookup.cache.ttl' = '100000',\n" +
				"  'lookup.max-retries' = '3'\n" +
				")");
		
		tEnv.createTemporarySystemFunction("mt3101_JSON_TO_ROW_IN_MT3101", JsonToRowInMt3101.class);
		tEnv.createTemporarySystemFunction("mt3101_JSON_TO_ROW_IN_MT9999", JsonToRowInMt9999.class);
		
		tEnv.executeSql("" +
				"CREATE VIEW MT3101 AS\n" +
				"  SELECT\n" +
				"  mt3101_JSON_TO_ROW_IN_MT3101(parseData) AS Manifest,\n" +
				"  LASTUPDATEDDT\n" +
				"  FROM DATA_XPQ_MSG_PARSE_RESULT\n" +
				"  WHERE msgType = 'message_data' AND bizId = 'MT3101'");
		
//		Table MT3101 = tEnv.sqlQuery("select * from MT3101");
//		tEnv.toAppendStream(MT3101, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW MT9999 AS\n" +
				"  SELECT\n" +
				"  mt3101_JSON_TO_ROW_IN_MT9999(parseData) AS Manifest,\n" +
				"  LASTUPDATEDDT\n" +
				"  FROM DATA_XPQ_MSG_PARSE_RESULT\n" +
				"  WHERE msgType = 'message_data' AND bizId = 'MT9999'");
		
//		Table MT9999 = tEnv.sqlQuery("select * from MT9999");
//		tEnv.toAppendStream(MT9999, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW MT3101consignment_TransportEquipment AS\n" +
				"select\n" +
				"  Manifest.Head.MessageID as MessageID,\n" +
				"  Manifest.Head.FunctionCode as FunctionCode,\n" +
				"  Manifest.Head.SendTime as SendTime,\n" +
				"  if(Manifest.Declaration.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(Manifest.Declaration.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,\n" +
				"  if(Manifest.Declaration.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(Manifest.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))), 'UN', ''), 'N/A') as BorderTransportMeans_ID,\n" +
				"  if(Manifest.Declaration.BorderTransportMeans.Name <> '', UPPER(TRIM(REGEXP_REPLACE(Manifest.Declaration.BorderTransportMeans.Name, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_Name,\n" +
				"  Manifest.Declaration.UnloadingLocation.ArrivalDate as UnloadingLocation_ArrivalDate,\n" +
				"  Manifest.Declaration.Consignment as Consignment,\n" +
				"  Manifest.Declaration.TransportEquipment as TransportEquipment,\n" +
				"  LASTUPDATEDDT,uuid() as UUID\n" +
				"from MT3101");
		
//		Table MT3101consignment_TransportEquipment = tEnv.sqlQuery("select * from MT3101consignment_TransportEquipment");
//		tEnv.toAppendStream(MT3101consignment_TransportEquipment, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view mt3101ConsignmentTable as\n" +
				"select\n" +
				"  MessageID, FunctionCode, SendTime,\n" +
				"  BorderTransportMeans_JourneyID, BorderTransportMeans_ID,\n" +
				"  BorderTransportMeans_Name, UnloadingLocation_ArrivalDate,\n" +
				"  if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as TransportContractDocument_ID, \n" +
				"  if(AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as AssociatedTransportDocument_ID, \n" +
				"  LASTUPDATEDDT,UUID\n" +
				"from (select \n" +
				"        MessageID,FunctionCode,SendTime,BorderTransportMeans_JourneyID,BorderTransportMeans_ID,\n" +
				"        BorderTransportMeans_Name,UnloadingLocation_ArrivalDate,Consignment,LASTUPDATEDDT,UUID\n" +
				"      from MT3101consignment_TransportEquipment where Consignment is not null) as m3c \n" +
				"CROSS JOIN UNNEST(Consignment) AS Consignment(TransportContractDocument, AssociatedTransportDocument,ConsignmentPackaging, TotalGrossMassMeasure, ConsignmentItem)");
		
//		Table mt3101ConsignmentTable = tEnv.sqlQuery("select * from mt3101ConsignmentTable");
//		tEnv.toAppendStream(mt3101ConsignmentTable, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view mt3101TransportEquipmentTable as\n" +
				"select\n" +
				"  MessageID, FunctionCode, SendTime, BorderTransportMeans_JourneyID, BorderTransportMeans_ID,\n" +
				"  BorderTransportMeans_Name, UnloadingLocation_ArrivalDate, LASTUPDATEDDT,\n" +
				"  if(EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as EquipmentIdentification_ID,  UUID\n" +
				"from (select MessageID,FunctionCode,SendTime,BorderTransportMeans_JourneyID,BorderTransportMeans_ID,BorderTransportMeans_Name,UnloadingLocation_ArrivalDate,TransportEquipment,LASTUPDATEDDT,UUID \n" +
				"      from MT3101consignment_TransportEquipment where TransportEquipment is not null) as m3t \n" +
				"CROSS JOIN UNNEST(TransportEquipment) AS TransportEquipment(EquipmentIdentification,CharacteristicCode,FullnessCode,SealID)");
		
//		Table mt3101TransportEquipmentTable = tEnv.sqlQuery("select * from mt3101TransportEquipmentTable");
//		tEnv.toAppendStream(mt3101TransportEquipmentTable, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view MT9999consignment_TransportEquipment as\n" +
				"select\n" +
				"  Manifest.Head.MessageID as MessageID,\n" +
				"  Manifest.Head.FunctionCode as FunctionCode,\n" +
				"  Manifest.Head.SendTime as SendTime,\n" +
				"  if(Manifest.Response.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(Manifest.Response.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))), 'UN', ''), 'N/A') as BorderTransportMeans_ID,\n" +
				"  if(Manifest.Response.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(Manifest.Response.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,\n" +
				"  Manifest.Response.Consignment as Consignment,\n" +
				"  Manifest.Response.TransportEquipment as TransportEquipment,\n" +
				"  LASTUPDATEDDT,uuid() as UUID\n" +
				"from MT9999 where Manifest.Head.MessageType='MT3101'");
		
//		Table MT9999consignment_TransportEquipment = tEnv.sqlQuery("select * from MT9999consignment_TransportEquipment");
//		tEnv.toAppendStream(MT9999consignment_TransportEquipment, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view mt9999ConsignmentTable as\n" +
				"select\n" +
				"  MessageID, FunctionCode, SendTime, BorderTransportMeans_ID, BorderTransportMeans_JourneyID,\n" +
				"  if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as TransportContractDocument_ID,\n" +
				"  if(AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as AssociatedTransportDocument_ID,\n" +
				"  ResponseType, LASTUPDATEDDT,UUID\n" +
				"from (select MessageID,FunctionCode,SendTime,BorderTransportMeans_ID,BorderTransportMeans_JourneyID,\n" +
				"  Consignment,LASTUPDATEDDT,UUID from MT9999consignment_TransportEquipment where Consignment is not null) as m9c \n" +
				"CROSS JOIN UNNEST(Consignment) AS Consignment(TransportContractDocument,AssociatedTransportDocument,ResponseType)");
		
//		Table mt9999ConsignmentTable = tEnv.sqlQuery("select * from mt9999ConsignmentTable");
//		tEnv.toAppendStream(mt9999ConsignmentTable, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view mt9999TransportEquipmentTable as\n" +
				"select\n" +
				"  MessageID, FunctionCode, SendTime, BorderTransportMeans_ID,\n" +
				"  BorderTransportMeans_JourneyID,ResponseType, LASTUPDATEDDT,\n" +
				"   if(EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as EquipmentIdentification_ID,  UUID\n" +
				"from (select MessageID,FunctionCode,SendTime,BorderTransportMeans_ID,BorderTransportMeans_JourneyID,TransportEquipment,LASTUPDATEDDT,UUID \n" +
				"      from MT9999consignment_TransportEquipment where TransportEquipment is not null) as m9t \n" +
				"CROSS JOIN UNNEST(TransportEquipment) AS TransportEquipment(EquipmentIdentification,ResponseType)");
		
//		Table mt9999TransportEquipmentTable = tEnv.sqlQuery("select * from mt9999TransportEquipmentTable");
//		tEnv.toAppendStream(mt9999TransportEquipmentTable, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view bill_temp as\n" +
				"select billtb.*,\n" +
				"  if(d1.res <> '', d1.res, if(d5.res <> '', d5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO号\n" +
				"  if(d2.res <> '', d2.res, if(d6.res <> '', d6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(d3.res <> '', d3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  if(d4.res <> '', d4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
				"from\n" +
				"  (select\n" +
				"    m3.BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO编号\n" +
				"    m3.BorderTransportMeans_Name as VSL_NAME, --船名\n" +
				"    m3.BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"    m3.TransportContractDocument_ID as BL_NO, --提运单号\n" +
				"    if(m3.FunctionCode = '3', 1, 0) as ISDELETED, --标记是否删除\n" +
				"    TO_TIMESTAMP(concat(substr(m9.SendTime,1,14),'.',substr(m9.SendTime,15)),'yyyyMMddHHmmss.SSS') as BIZ_TIME, --业务发生时间\n" +
				"    if(m9.ResponseType.Code <> '', m9.ResponseType.Code, 'N/A') as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(m9.ResponseType.Text <> '', m9.ResponseType.Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    m3.LASTUPDATEDDT, --最后处理时间\n" +
				"    'C6.11' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'E_cusDecl_mt3101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    'N/A' as MASTER_BL_NO, --总提运单号\n" +
				"    'E' as I_E_MARK, --进出口标记\n" +
				"    m3.UUID,\n" +
				"    if(m9.ResponseType.Code='01' and m9.ResponseType.Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"  from mt3101ConsignmentTable as m3 join mt9999ConsignmentTable as m9\n" +
				"    on m3.MessageID = m9.MessageID\n" +
				"    and m3.BorderTransportMeans_ID = m9.BorderTransportMeans_ID\n" +
				"    and m3.BorderTransportMeans_JourneyID = m9.BorderTransportMeans_JourneyID\n" +
				"    and m3.TransportContractDocument_ID = m9.TransportContractDocument_ID\n" +
				"    and m3.AssociatedTransportDocument_ID = m9.AssociatedTransportDocument_ID\n" +
				"  ) as billtb\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',billtb.VSL_IMO_NO)=d1.key and 'IMO_NO'=d1.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',billtb.VSL_IMO_NO)=d2.key and 'VSL_NAME_EN'=d2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',billtb.VSL_NAME)=d5.key and 'IMO_NO'=d5.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',billtb.VSL_NAME)=d6.key and 'VSL_NAME_EN'=d6.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',billtb.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',billtb.BIZ_STAGE_CODE)=d3.key and 'SUB_STAGE_NAME'=d3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF billtb.LASTUPDATEDDT as d4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',billtb.BIZ_STATUS_CODE)=d4.key and 'TYPE_NAME'=d4.hashkey");
		
//		Table bill_temp = tEnv.sqlQuery("select * from bill_temp");
//		tEnv.toAppendStream(bill_temp, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view ctn_temp as\n" +
				"select ctntb.*,\n" +
				"  if(d1.res <> '', d1.res, if(d5.res <> '', d5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO号\n" +
				"  if(d2.res <> '', d2.res, if(d6.res <> '', d6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(d3.res <> '', d3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  if(d4.res <> '', d4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
				"from\n" +
				"  (select\n" +
				"    m3t.BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO编号\n" +
				"    m3t.BorderTransportMeans_Name as VSL_NAME, --船名\n" +
				"    m3t.BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"    m3t.EquipmentIdentification_ID as CTNR_NO, --箱号\n" +
				"    if(m3t.FunctionCode = '3', 1, 0) as ISDELETED, --标记是否删除\n" +
				"    TO_TIMESTAMP(concat(substr(m9t.SendTime,1,14),'.',substr(m9t.SendTime,15)),'yyyyMMddHHmmss.SSS') as BIZ_TIME, --业务发生时间\n" +
				"    if(m9t.ResponseType.Code <> '', m9t.ResponseType.Code, 'N/A') as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(m9t.ResponseType.Text <> '', m9t.ResponseType.Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    m3t.LASTUPDATEDDT, --最后处理时间\n" +
				"    'C6.11' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'E_cusDecl_mt3101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    'E' as I_E_MARK, --进出口标记\n" +
				"    m3t.UUID,\n" +
				"    if(m9t.ResponseType.Code='01' and m9t.ResponseType.Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"  from mt3101TransportEquipmentTable as m3t join mt9999TransportEquipmentTable as m9t\n" +
				"    on m3t.MessageID = m9t.MessageID\n" +
				"    and m3t.BorderTransportMeans_ID = m9t.BorderTransportMeans_ID\n" +
				"    and m3t.BorderTransportMeans_JourneyID = m9t.BorderTransportMeans_JourneyID\n" +
				"    and m3t.EquipmentIdentification_ID = m9t.EquipmentIdentification_ID\n" +
				"  ) as ctntb\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',ctntb.VSL_IMO_NO)=d1.key and 'IMO_NO'=d1.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',ctntb.VSL_IMO_NO)=d2.key and 'VSL_NAME_EN'=d2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',ctntb.VSL_NAME)=d5.key and 'IMO_NO'=d5.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',ctntb.VSL_NAME)=d6.key and 'VSL_NAME_EN'=d6.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',ctntb.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',ctntb.BIZ_STAGE_CODE)=d3.key and 'SUB_STAGE_NAME'=d3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ctntb.LASTUPDATEDDT as d4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',ctntb.BIZ_STATUS_CODE)=d4.key and 'TYPE_NAME'=d4.hashkey");
		
//		Table ctn_temp = tEnv.sqlQuery("select * from ctn_temp");
//		tEnv.toAppendStream(ctn_temp, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view ctn_join_billctn as\n" +
				"select \n" +
				"  ctn_t1.VSL_IMO_NO, ctn_t1.VSL_NAME, ctn_t1.VOYAGE,\n" +
				"  ctn_t1.ACCURATE_IMONO, ctn_t1.ACCURATE_VSLNAME, obcd.BL_NO,\n" +
				"  obcd.MASTER_BL_NO, ctn_t1.I_E_MARK, ctn_t1.BIZ_STAGE_NO,\n" +
				"  ctn_t1.BIZ_STAGE_CODE, ctn_t1.BIZ_STAGE_NAME, ctn_t1.BIZ_TIME,\n" +
				"  ctn_t1.BIZ_STATUS_CODE, ctn_t1.BIZ_STATUS, ctn_t1.BIZ_STATUS_DESC,\n" +
				"  ctn_t1.LASTUPDATEDDT, ctn_t1.ISDELETED, UUID, BIZ_STATUS_IFFECTIVE\n" +
				"from (select * from ctn_temp where BIZ_STATUS_CODE='01') as ctn_t1 left join oracle_blctnr_dim FOR SYSTEM_TIME as OF ctn_t1.LASTUPDATEDDT as obcd\n" +
				"  on ctn_t1.VSL_IMO_NO=obcd.VSL_IMO_NO and ctn_t1.VOYAGE=obcd.VOYAGE and ctn_t1.CTNR_NO=obcd.CTNR_NO\n" +
				"where obcd.ISDELETED=0");
		
//		Table ctn_join_billctn = tEnv.sqlQuery("select * from ctn_join_billctn");
//		tEnv.toAppendStream(ctn_join_billctn, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view inport as\n" +
				"select ipt.*, --报文字段和固定值字段\n" +
				"  if(id1.res <> '', id1.res, if(id5.res <> '', id5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO编号\n" +
				"  if(id2.res <> '', id2.res, if(id6.res <> '', id6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(id3.res <> '', id3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  if(id4.res <> '', id4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
				"from\n" +
				"  (select\n" +
				"    MessageID,--给后面关联\n" +
				"    AssociatedTransportDocument_ID,--给后面关联\n" +
				"    BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO编号\n" +
				"    BorderTransportMeans_Name as VSL_NAME, --船名\n" +
				"    BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"    TransportContractDocument_ID as BL_NO, --提运单号\n" +
				"    TO_TIMESTAMP(concat(UnloadingLocation_ArrivalDate, '000000.000'), 'yyyyMMddHHmmss.SSS') as BIZ_TIME, --业务发生时间\n" +
				"    if(FunctionCode = '3', 1, 0) as ISDELETED, --标记是否删除\n" +
				"    'N/A' as MASTER_BL_NO, --总提运单号\n" +
				"    'E' as I_E_MARK, --进出口标记\n" +
				"    'C6.1s' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'E_portIn_costrpBulk' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    '1' as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    'N/A' as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    LASTUPDATEDDT, --最后处理时间\n" +
				"    UUID,\n" +
				"    1 as BIZ_STATUS_IFFECTIVE\n" +
				"  from mt3101ConsignmentTable) as ipt\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',ipt.VSL_IMO_NO)=id1.key and 'IMO_NO'=id1.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',ipt.VSL_IMO_NO)=id2.key and 'VSL_NAME_EN'=id2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',ipt.VSL_NAME)=id5.key and 'IMO_NO'=id5.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',ipt.VSL_NAME)=id6.key and 'VSL_NAME_EN'=id6.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',ipt.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',ipt.BIZ_STAGE_CODE)=id3.key and 'SUB_STAGE_NAME'=id3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF ipt.LASTUPDATEDDT as id4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=port_in_status&TYPE_CODE=',ipt.BIZ_STATUS_CODE)=id4.key and 'TYPE_NAME'=id4.hashkey");
		
//		Table inport = tEnv.sqlQuery("select * from inport");
//		tEnv.toAppendStream(inport, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view use999timeinport as\n" +
				"select uin.VSL_IMO_NO,uin.VSL_NAME,uin.VOYAGE,uin.ACCURATE_IMONO,uin.ACCURATE_VSLNAME,\n" +
				"  uin.BL_NO,uin.MASTER_BL_NO,uin.I_E_MARK,uin.BIZ_STAGE_NO,uin.BIZ_STAGE_CODE,uin.BIZ_STAGE_NAME,\n" +
				"  TO_TIMESTAMP(concat(substr(mt9in.SendTime,1,14),'.',substr(mt9in.SendTime,15)),'yyyyMMddHHmmss.SSS') as BIZ_TIME,\n" +
				"  uin.BIZ_STATUS_CODE,uin.BIZ_STATUS,uin.BIZ_STATUS_DESC,uin.LASTUPDATEDDT,uin.ISDELETED, uin.UUID, uin.BIZ_STATUS_IFFECTIVE\n" +
				"from (select * from inport where BIZ_TIME is null) as uin join mt9999ConsignmentTable as mt9in\n" +
				"  on uin.MessageID=mt9in.MessageID\n" +
				"  and uin.VSL_IMO_NO=mt9in.BorderTransportMeans_ID\n" +
				"  and uin.VOYAGE=mt9in.BorderTransportMeans_JourneyID\n" +
				"  and uin.BL_NO=mt9in.TransportContractDocument_ID\n" +
				"  and uin.AssociatedTransportDocument_ID=mt9in.AssociatedTransportDocument_ID");
		
//		Table use999timeinport = tEnv.sqlQuery("select * from use999timeinport");
//		tEnv.toAppendStream(use999timeinport, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_track_biz_status_bill (\n" +
				"  VSL_IMO_NO STRING,\n" +
				"  VSL_NAME STRING,\n" +
				"  VOYAGE STRING,\n" +
				"  ACCURATE_IMONO STRING,\n" +
				"  ACCURATE_VSLNAME STRING,\n" +
				"  BL_NO STRING,\n" +
				"  MASTER_BL_NO STRING,\n" +
				"  I_E_MARK STRING,\n" +
				"  BIZ_STAGE_NO STRING,\n" +
				"  BIZ_STAGE_CODE STRING,\n" +
				"  BIZ_STAGE_NAME STRING,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE STRING,\n" +
				"  BIZ_STATUS STRING,\n" +
				"  BIZ_STATUS_DESC STRING,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  UUID STRING,\n" +
				"  BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO,BIZ_STATUS_CODE) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE oracle_track_biz_status_ctnr (\n" +
				"  VSL_IMO_NO STRING,\n" +
				"  VSL_NAME STRING,\n" +
				"  VOYAGE STRING,\n" +
				"  ACCURATE_IMONO STRING,\n" +
				"  ACCURATE_VSLNAME STRING,\n" +
				"  CTNR_NO STRING,\n" +
				"  I_E_MARK STRING,\n" +
				"  BIZ_STAGE_NO STRING,\n" +
				"  BIZ_STAGE_CODE STRING,\n" +
				"  BIZ_STAGE_NAME STRING,\n" +
				"  BIZ_TIME TIMESTAMP,\n" +
				"  BIZ_STATUS_CODE STRING,\n" +
				"  BIZ_STATUS STRING,\n" +
				"  BIZ_STATUS_DESC STRING,\n" +
				"  LASTUPDATEDDT TIMESTAMP,\n" +
				"  ISDELETED DECIMAL(22, 0),\n" +
				"  UUID STRING,\n" +
				"  BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"  PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
				") WITH (\n" +
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
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
				"  'topic' = 'topic-bdpevent-flink-push-mt3101',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
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
				"  'topic' = 'topic-bdpevent-flink-push-mt3101',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,\n" +
				"  MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select \n" +
				"  bt.VSL_IMO_NO,bt.VSL_NAME,bt.VOYAGE,bt.ACCURATE_IMONO,bt.ACCURATE_VSLNAME,\n" +
				"  bt.BL_NO,bt.MASTER_BL_NO,bt.I_E_MARK,bt.BIZ_STAGE_NO,bt.BIZ_STAGE_CODE,\n" +
				"  bt.BIZ_STAGE_NAME,bt.BIZ_TIME,bt.BIZ_STATUS_CODE,bt.BIZ_STATUS,bt.BIZ_STATUS_DESC,\n" +
				"  bt.LASTUPDATEDDT,bt.ISDELETED,concat(bt.UUID,'_',bt.BL_NO,'_',cast(bt.BIZ_TIME as STRING)) as UUID,bt.BIZ_STATUS_IFFECTIVE\n" +
				"from bill_temp as bt left join oracle_bill_dim FOR SYSTEM_TIME as OF bt.LASTUPDATEDDT as obd \n" +
				"  on bt.VSL_IMO_NO=obd.VSL_IMO_NO \n" +
				"  and bt.VOYAGE=obd.VOYAGE \n" +
				"  and bt.BL_NO=obd.BL_NO \n" +
				"  and bt.BIZ_STAGE_NO=obd.BIZ_STAGE_NO \n" +
				"  and bt.BIZ_STATUS_CODE=obd.BIZ_STATUS_CODE\n" +
				"where (obd.BIZ_TIME is null or bt.BIZ_TIME>obd.BIZ_TIME) and bt.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select UUID as GID, 'DATA_FLINK_FULL_FLINK_TRACING_MT3101' as APP_NAME, 'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,\n" +
				"    I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,\n" +
				"    BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE\n" +
				"  ) as DATA\n" +
				"from\n" +
				"  (select bts.VSL_IMO_NO,bts.VSL_NAME,bts.VOYAGE,bts.ACCURATE_IMONO,bts.ACCURATE_VSLNAME,bts.BL_NO,\n" +
				"    bts.MASTER_BL_NO,bts.I_E_MARK,bts.BIZ_STAGE_NO,bts.BIZ_STAGE_CODE,bts.BIZ_STAGE_NAME,bts.BIZ_TIME,\n" +
				"    bts.BIZ_STATUS_CODE,bts.BIZ_STATUS,bts.BIZ_STATUS_DESC,bts.LASTUPDATEDDT,bts.ISDELETED,\n" +
				"    concat(bts.UUID,'_',bts.BL_NO,'_',cast(bts.BIZ_TIME as STRING)) as UUID,bts.BIZ_STATUS_IFFECTIVE\n" +
				"  from bill_temp as bts left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bts.LASTUPDATEDDT as ospd \n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_MT3101'=ospd.APP_NAME AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd.TABLE_NAME \n" +
				"  where ospd.ISCURRENT=1 and bts.BIZ_TIME is not null  ) as k1");
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_ctnr (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select ct.VSL_IMO_NO,ct.VSL_NAME,ct.VOYAGE,ct.ACCURATE_IMONO,ct.ACCURATE_VSLNAME,ct.CTNR_NO,\n" +
				"  ct.I_E_MARK,ct.BIZ_STAGE_NO,ct.BIZ_STAGE_CODE,ct.BIZ_STAGE_NAME,ct.BIZ_TIME,\n" +
				"  ct.BIZ_STATUS_CODE,ct.BIZ_STATUS,ct.BIZ_STATUS_DESC,ct.LASTUPDATEDDT,ct.ISDELETED,\n" +
				"  concat(ct.UUID,'_',ct.CTNR_NO,'_',cast(ct.BIZ_TIME as STRING)) as UUID,ct.BIZ_STATUS_IFFECTIVE\n" +
				"from ctn_temp as ct left join oracle_ctnr_dim FOR SYSTEM_TIME as OF ct.LASTUPDATEDDT as ocd \n" +
				"  on ct.VSL_IMO_NO=ocd.VSL_IMO_NO \n" +
				"  and ct.VOYAGE=ocd.VOYAGE and ct.CTNR_NO=ocd.CTNR_NO \n" +
				"  and ct.BIZ_STAGE_NO=ocd.BIZ_STAGE_NO \n" +
				"  and ct.BIZ_STATUS_CODE=ocd.BIZ_STATUS_CODE\n" +
				"where (ocd.BIZ_TIME is null or ct.BIZ_TIME>ocd.BIZ_TIME) and ct.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_ctn (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select UUID as GID, 'DATA_FLINK_FULL_FLINK_TRACING_MT3101' as APP_NAME, 'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,\n" +
				"    BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select cts.VSL_IMO_NO,cts.VSL_NAME,cts.VOYAGE,cts.ACCURATE_IMONO,cts.ACCURATE_VSLNAME,cts.CTNR_NO,\n" +
				"    cts.I_E_MARK,cts.BIZ_STAGE_NO,cts.BIZ_STAGE_CODE,cts.BIZ_STAGE_NAME,cts.BIZ_TIME,\n" +
				"    cts.BIZ_STATUS_CODE,cts.BIZ_STATUS,cts.BIZ_STATUS_DESC,cts.LASTUPDATEDDT,\n" +
				"    cts.ISDELETED,concat(cts.UUID,'_',cts.CTNR_NO,'_',cast(cts.BIZ_TIME as STRING)) as UUID,cts.BIZ_STATUS_IFFECTIVE\n" +
				"  from ctn_temp as cts left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF cts.LASTUPDATEDDT as ospd \n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_MT3101'=ospd.APP_NAME \n" +
				"    and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd.TABLE_NAME \n" +
				"  where ospd.ISCURRENT=1 and cts.BIZ_TIME is not null) as c1s");
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select \n" +
				"  bt2.VSL_IMO_NO,bt2.VSL_NAME,bt2.VOYAGE,bt2.ACCURATE_IMONO,bt2.ACCURATE_VSLNAME,\n" +
				"  bt2.BL_NO,bt2.MASTER_BL_NO,bt2.I_E_MARK,bt2.BIZ_STAGE_NO,bt2.BIZ_STAGE_CODE,\n" +
				"  bt2.BIZ_STAGE_NAME,bt2.BIZ_TIME,bt2.BIZ_STATUS_CODE,bt2.BIZ_STATUS,\n" +
				"  bt2.BIZ_STATUS_DESC,bt2.LASTUPDATEDDT,bt2.ISDELETED,concat(bt2.UUID,'_',bt2.BL_NO,'_',cast(bt2.BIZ_TIME as STRING)) as UUID,bt2.BIZ_STATUS_IFFECTIVE\n" +
				"from ctn_join_billctn as bt2 left join oracle_bill_dim FOR SYSTEM_TIME as OF bt2.LASTUPDATEDDT as obd \n" +
				"  on bt2.VSL_IMO_NO=obd.VSL_IMO_NO \n" +
				"  and bt2.VOYAGE=obd.VOYAGE \n" +
				"  and bt2.BL_NO=obd.BL_NO \n" +
				"  and bt2.BIZ_STAGE_NO=obd.BIZ_STAGE_NO \n" +
				"  and bt2.BIZ_STATUS_CODE=obd.BIZ_STATUS_CODE\n" +
				"where (obd.BIZ_TIME is null or bt2.BIZ_TIME>obd.BIZ_TIME) and bt2.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select UUID as GID, 'DATA_FLINK_FULL_FLINK_TRACING_MT3101' as APP_NAME, \n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,\n" +
				"    MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"    BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,\n" +
				"    ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select \n" +
				"    bt1.VSL_IMO_NO,bt1.VSL_NAME,bt1.VOYAGE,bt1.ACCURATE_IMONO,bt1.ACCURATE_VSLNAME,bt1.BL_NO,\n" +
				"    bt1.MASTER_BL_NO,bt1.I_E_MARK,bt1.BIZ_STAGE_NO,bt1.BIZ_STAGE_CODE,bt1.BIZ_STAGE_NAME,\n" +
				"    bt1.BIZ_TIME,bt1.BIZ_STATUS_CODE,bt1.BIZ_STATUS,bt1.BIZ_STATUS_DESC,bt1.LASTUPDATEDDT,\n" +
				"    bt1.ISDELETED,concat(bt1.UUID,'_',bt1.BL_NO,'_',cast(bt1.BIZ_TIME as STRING)) as UUID,bt1.BIZ_STATUS_IFFECTIVE\n" +
				"  from ctn_join_billctn as bt1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bt1.LASTUPDATEDDT as ospd \n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_MT3101'=ospd.APP_NAME AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd.TABLE_NAME \n" +
				"  where ospd.ISCURRENT=1 and bt1.BIZ_TIME is not null) as k2");
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select in1.VSL_IMO_NO,in1.VSL_NAME,in1.VOYAGE,in1.ACCURATE_IMONO,in1.ACCURATE_VSLNAME,\n" +
				"  in1.BL_NO,in1.MASTER_BL_NO,in1.I_E_MARK,in1.BIZ_STAGE_NO,in1.BIZ_STAGE_CODE,\n" +
				"  in1.BIZ_STAGE_NAME,in1.BIZ_TIME,in1.BIZ_STATUS_CODE,in1.BIZ_STATUS,\n" +
				"  in1.BIZ_STATUS_DESC,in1.LASTUPDATEDDT,in1.ISDELETED,\n" +
				"  concat(in1.UUID,'_',in1.BL_NO,'_',in1.BIZ_STAGE_NO,'_',cast(in1.BIZ_TIME as STRING)) as UUID,\n" +
				"  in1.BIZ_STATUS_IFFECTIVE\n" +
				"from inport as in1 left join oracle_bill_dim FOR SYSTEM_TIME as OF in1.LASTUPDATEDDT as obd1\n" +
				"  on in1.VSL_IMO_NO=obd1.VSL_IMO_NO\n" +
				"  and in1.VOYAGE=obd1.VOYAGE\n" +
				"  and in1.BL_NO=obd1.BL_NO\n" +
				"  and in1.BIZ_STAGE_NO=obd1.BIZ_STAGE_NO\n" +
				"  and in1.BIZ_STATUS_CODE=obd1.BIZ_STATUS_CODE\n" +
				"where (obd1.BIZ_TIME is null or in1.BIZ_TIME>obd1.BIZ_TIME) and in1.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select UUID as GID, 'DATA_FLINK_FULL_FLINK_TRACING_COSTRPBULK' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"    BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"    BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select ibts.VSL_IMO_NO,ibts.VSL_NAME,ibts.VOYAGE,ibts.ACCURATE_IMONO,\n" +
				"    ibts.ACCURATE_VSLNAME,ibts.BL_NO,ibts.MASTER_BL_NO,ibts.I_E_MARK,ibts.BIZ_STAGE_NO,\n" +
				"    ibts.BIZ_STAGE_CODE,ibts.BIZ_STAGE_NAME,ibts.BIZ_TIME,ibts.BIZ_STATUS_CODE,ibts.BIZ_STATUS,\n" +
				"    ibts.BIZ_STATUS_DESC,ibts.LASTUPDATEDDT,ibts.ISDELETED,\n" +
				"    concat(ibts.UUID,'_',ibts.BL_NO,'_',ibts.BIZ_STAGE_NO,'_',cast(ibts.BIZ_TIME as STRING)) as UUID,\n" +
				"    ibts.BIZ_STATUS_IFFECTIVE\n" +
				"  from inport as ibts left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF ibts.LASTUPDATEDDT as ospd \n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_COSTRPBULK'=ospd.APP_NAME AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd.TABLE_NAME \n" +
				"  where ospd.ISCURRENT=1 and ibts.BIZ_TIME is not null) as ik1");
		
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"  BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,\n" +
				"  BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select   in2.VSL_IMO_NO,in2.VSL_NAME,in2.VOYAGE,in2.ACCURATE_IMONO,in2.ACCURATE_VSLNAME,\n" +
				"  in2.BL_NO,in2.MASTER_BL_NO,in2.I_E_MARK,in2.BIZ_STAGE_NO,in2.BIZ_STAGE_CODE,\n" +
				"  in2.BIZ_STAGE_NAME,in2.BIZ_TIME,in2.BIZ_STATUS_CODE,in2.BIZ_STATUS,in2.BIZ_STATUS_DESC,\n" +
				"  in2.LASTUPDATEDDT,in2.ISDELETED,\n" +
				"  concat(in2.UUID,'_',in2.BL_NO,'_',in2.BIZ_STAGE_NO,'_',cast(in2.BIZ_TIME as STRING)) as UUID,\n" +
				"  in2.BIZ_STATUS_IFFECTIVE\n" +
				"from use999timeinport as in2 left join oracle_bill_dim FOR SYSTEM_TIME as OF in2.LASTUPDATEDDT as obd2 \n" +
				"  on in2.VSL_IMO_NO=obd2.VSL_IMO_NO \n" +
				"  and in2.VOYAGE=obd2.VOYAGE \n" +
				"  and in2.BL_NO=obd2.BL_NO \n" +
				"  and in2.BIZ_STAGE_NO=obd2.BIZ_STAGE_NO \n" +
				"  and in2.BIZ_STATUS_CODE=obd2.BIZ_STATUS_CODE\n" +
				"where (obd2.BIZ_TIME is null or in2.BIZ_TIME>obd2.BIZ_TIME) and in2.BIZ_TIME is not null");
		
		statementSet.addInsertSql("" +
				"insert into kafka_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select UUID as GID, 'DATA_FLINK_FULL_FLINK_TRACING_COSTRPBULK' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  Row(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"    BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,\n" +
				"    BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"  (select ibts1.VSL_IMO_NO,ibts1.VSL_NAME,ibts1.VOYAGE,ibts1.ACCURATE_IMONO,\n" +
				"    ibts1.ACCURATE_VSLNAME,ibts1.BL_NO,ibts1.MASTER_BL_NO,ibts1.I_E_MARK,ibts1.BIZ_STAGE_NO,\n" +
				"    ibts1.BIZ_STAGE_CODE,ibts1.BIZ_STAGE_NAME,ibts1.BIZ_TIME,ibts1.BIZ_STATUS_CODE,ibts1.BIZ_STATUS,\n" +
				"    ibts1.BIZ_STATUS_DESC,ibts1.LASTUPDATEDDT,ibts1.ISDELETED,\n" +
				"    concat(ibts1.UUID,'_',ibts1.BL_NO,'_',ibts1.BIZ_STAGE_NO,'_',cast(ibts1.BIZ_TIME as STRING)) as UUID,\n" +
				"    ibts1.BIZ_STATUS_IFFECTIVE\n" +
				"  from use999timeinport as ibts1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF ibts1.LASTUPDATEDDT as ospd2\n" +
				"    on 'DATA_FLINK_FULL_FLINK_TRACING_COSTRPBULK'=ospd2.APP_NAME \n" +
				"    AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME \n" +
				"  where ospd2.ISCURRENT=1 and ibts1.BIZ_TIME is not null\n" +
				"  ) as ik2");
		
		statementSet.execute();
	}
}
