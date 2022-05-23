package com.huc.flinksql_mt1101;

import com.easipass.flink.table.function.udsf.JsonToRowInMT1101;
import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class mt1101 {
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
				"  'topic' = 'mt1101_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'mt1101_test',\n" +
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
		
		// TODO Oracle 箱单关系表 dim
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
		
		// TODO 注册1101解析函数
		tEnv.createTemporarySystemFunction("parseMT1101", JsonToRowInMT1101.class);
		
		// TODO 解析mt1101
		tEnv.executeSql("" +
				"create view source1101TB as\n" +
				"select msgId,LASTUPDATEDDT,parseMT1101(parseData) as pData,concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
				"from kafka_source_data where bizId='MT1101' and msgType='message_data'");

//		Table source1101TB = tEnv.sqlQuery("select * from source1101TB");
//		tEnv.toAppendStream(source1101TB, Row.class).print();
//		env.execute();
		
		// TODO 1101获取报文公共字段
		tEnv.executeSql("" +
				"create view common1101TB as \n" +
				"select \n" +
				"  msgId, GID, LASTUPDATEDDT,\n" +
				"  pData.Head.MessageID as Head_MessageID,\n" +
				"  if(pData.Declaration.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO,\n" +
				"  if(pData.Declaration.BorderTransportMeans.Name <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.Name, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"  if(pData.Declaration.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  'I' as I_E_MARK,\n" +
				"  'D6.1' as BIZ_STAGE_NO,\n" +
				"  'I_cusDecl_mt1101' as BIZ_STAGE_CODE,\n" +
				"  pData.Head.FunctionCode as FunctionCode,--用于箱单关系表\n" +
				"  pData.ExtraInfo.sender as SenderID,--用于箱单关系表\n" +
				"  if(pData.Head.FunctionCode = '3', 1, 0) as ISDELETED,\n" +
				"  pData.Declaration.Consignment as Consignment\n" +
				"from source1101TB");

//		Table common1101TB = tEnv.sqlQuery("select * from common1101TB");
//		tEnv.toAppendStream(common1101TB, Row.class).print();
//		env.execute();
		
		// TODO 展开1101提单
		tEnv.executeSql("" +
				"create view mt1101withBill as \n" +
				"select msgId, GID, LASTUPDATEDDT,Head_MessageID,VSL_IMO_NO,VSL_NAME,VOYAGE,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,FunctionCode,SenderID,ISDELETED,\n" +
				"  if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as TransportContractDocument_ID,\n" +
				"  if(AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as AssociatedTransportDocument_ID,\n" +
				"  TransportEquipment --箱,此处还未展开\n" +
				"from (select * from common1101TB where Consignment is not null) as tempTB1 cross join unnest(Consignment) AS Consignment(TransportContractDocument,AssociatedTransportDocument,LoadingLocation,UnloadingLocation,GoodsReceiptPlace,TranshipmentLocation,TransitDestination,GoodsConsignedPlace,CustomsStatusCode,FreightPayment,ConsignmentPackaging,TotalGrossMassMeasure,Consignee,Consignor,NotifyParty,UNDGContact,TransportEquipment,ConsignmentItem)");

//		Table mt1101withBill = tEnv.sqlQuery("select * from mt1101withBill");
//		tEnv.toAppendStream(mt1101withBill, Row.class).print();
//		env.execute();
		
		// TODO 注册9999解析函数
		tEnv.createTemporarySystemFunction("parseMT9999", JsonToRowInMt9999.class);
		
		// TODO 解析mt9999
		tEnv.executeSql("" +
				"create view source9999TB as\n" +
				"select msgId,LASTUPDATEDDT,parseMT9999(parseData) as pData,concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
				"from kafka_source_data where bizId='MT9999' and msgType='message_data'");

//		Table source9999TB = tEnv.sqlQuery("select * from source9999TB");
//		tEnv.toAppendStream(source9999TB, Row.class).print();
//		env.execute();
		
		// TODO 9999获取报文公共字段
		tEnv.executeSql("" +
				"create view common9999TB as \n" +
				"select \n" +
				"  msgId,GID,LASTUPDATEDDT,\n" +
				"  pData.Head.MessageID as Head_MessageID,\n" +
				"  pData.Head.MessageType as MessageType,\n" +
				"  if(pData.Response.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO,\n" +
				"  if(pData.Response.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Response.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  TO_TIMESTAMP(concat(substr(pData.Head.SendTime, 1, 14), '.', substr(pData.Head.SendTime, 15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME,\n" +
				"  if(pData.Head.FunctionCode = '3', 1, 0) as ISDELETED,\n" +
				"  pData.Response.Consignment as Consignment\n" +
				"from source9999TB where MessageType='MT1101'");

//		Table common9999TB = tEnv.sqlQuery("select * from common9999TB");
//		tEnv.toAppendStream(common9999TB, Row.class).print();
//		env.execute();
		
		// TODO 展开9999提单
		tEnv.executeSql("" +
				"create view mt9999withBill as \n" +
				"select msgId, GID, LASTUPDATEDDT, Head_MessageID, VSL_IMO_NO, VOYAGE, BIZ_TIME, ISDELETED, 'I' as I_E_MARK,\n" +
				"  if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as TransportContractDocument_ID,\n" +
				"  if(AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as AssociatedTransportDocument_ID,\n" +
				"  ResponseType.Code as BIZ_STATUS_CODE, if(ResponseType.Text <> '', ResponseType.Text, 'N/A') as BIZ_STATUS_DESC\n" +
				"from (select * from common9999TB where Consignment is not null) as tempTB2 cross join unnest(Consignment) AS Consignment(TransportContractDocument,AssociatedTransportDocument,ResponseType)");

//		Table mt9999withBill = tEnv.sqlQuery("select * from mt9999withBill");
//		tEnv.toAppendStream(mt9999withBill, Row.class).print();
//		env.execute();
		
		// TODO 关联维表取标准IMO、船名
		tEnv.executeSql("" +
				"create view mt9999WB_standerImoName as\n" +
				"select\n" +
				"  tm1.*,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_IMONO, --标准IMO\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as VSL_NAME,\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS, --业务状态\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  uuid() as UUID,\n" +
				"  if(BIZ_STATUS_CODE='01' and BIZ_STATUS_DESC not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from\n" +
				"  (select mt9999withBill.*,\n" +
				"     if(AssociatedTransportDocument_ID='N/A', 'D6.1', if(AssociatedTransportDocument_ID <> 'N/A' and Head_MessageID like 'D%', 'D6a.3', 'N/A')) as BIZ_STAGE_NO,\n" +
				"     if(AssociatedTransportDocument_ID='N/A', 'I_cusDecl_mt1101', if(AssociatedTransportDocument_ID <> 'N/A' and Head_MessageID like 'D%', 'I_cusDecl_mt1101Sub', 'N/A')) as BIZ_STAGE_CODE\n" +
				"  from mt9999withBill) as tm1\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tm1.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',tm1.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tm1.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',tm1.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tm1.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',tm1.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tm1.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',BIZ_STAGE_NO,'&SUB_STAGE_CODE=',BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey");

//		Table mt9999WB_standerImoName = tEnv.sqlQuery("select * from mt9999WB_standerImoName");
//		tEnv.toAppendStream(mt9999WB_standerImoName, Row.class).print();
//		env.execute();
		
		// TODO 原始舱单箱状态表、箱单关系表 关联redis
		tEnv.executeSql("" +
				"create view Mt1101JoinMt9999WithDimTB as\n" +
				"select tempTB3.*,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_IMONO, --标准IMO  if(dim5.res <> '', dim5.res, 'N/A')\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_VSLNAME, --标准船名  if(dim6.res <> '', dim6.res, 'N/A')\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务节点名称\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS, --业务状态\n" +
				"--  if(dim7.res <> '', dim7.res, 'N/A') as SENDER_CODE --发送方代码，弃用，改从报文取\n" +
				"  uuid() as UUID,\n" +
				"  if(BIZ_STATUS_CODE='01' and BIZ_STATUS_DESC not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from\n" +
				"(select mt1101withBill.msgId, mt1101withBill.GID, mt1101withBill.VSL_IMO_NO,\n" +
				"  mt1101withBill.VSL_NAME, mt1101withBill.VOYAGE,\n" +
				"  mt1101withBill.I_E_MARK, 'D6.1' as BIZ_STAGE_NO,\n" +
				"  'I_cusDecl_mt1101' as BIZ_STAGE_CODE, mt9999withBill.BIZ_TIME,\n" +
				"  mt9999withBill.BIZ_STATUS_CODE, mt9999withBill.BIZ_STATUS_DESC, \n" +
				"  mt1101withBill.LASTUPDATEDDT, mt1101withBill.ISDELETED,\n" +
				"  mt1101withBill.Head_MessageID, --报文编号,给箱单关系表用\n" +
				"  mt1101withBill.TransportContractDocument_ID, --总提单号,给箱单关系表用\n" +
				"  mt1101withBill.AssociatedTransportDocument_ID, --分提单号,给箱单关系表用\n" +
				"  mt1101withBill.SenderID as SENDER_CODE,--发送方代码,给箱单关系表用\n" +
				"  mt1101withBill.TransportEquipment, --箱,此处还未展开\n" +
				"  mt1101withBill.FunctionCode --报文功能代码,给箱单关系表用\n" +
				"from mt1101withBill join mt9999withBill\n" +
				"on mt1101withBill.Head_MessageID = mt9999withBill.Head_MessageID\n" +
				"and mt1101withBill.VSL_IMO_NO = mt9999withBill.VSL_IMO_NO\n" +
				"and mt1101withBill.VOYAGE = mt9999withBill.VOYAGE\n" +
				"and mt1101withBill.TransportContractDocument_ID = mt9999withBill.TransportContractDocument_ID) as tempTB3\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tempTB3.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',tempTB3.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tempTB3.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',tempTB3.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tempTB3.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',tempTB3.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',tempTB3.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF tempTB3.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',tempTB3.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");

//		Table Mt1101JoinMt9999WithDimTB = tEnv.sqlQuery("select * from Mt1101JoinMt9999WithDimTB");
//		tEnv.toAppendStream(Mt1101JoinMt9999WithDimTB, Row.class).print();
//		env.execute();
		
		// TODO 展开箱，获取箱表和箱单关系表,若箱为空，这条数据也要入箱单关系表，箱号为N/A，但不入箱状态表
		// TODO 结果表：原始舱单D6.1，I_cusDecl_mt1101箱状态表，insert时需要过滤掉箱号为N/A的
		tEnv.executeSql("" +
				"create view Mt1101JoinMt9999WithDim_ctnrTB as \n" +
				"select msgId,GID,VSL_IMO_NO,VSL_NAME,VOYAGE,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_TIME, \n" +
				"  BIZ_STATUS_CODE,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE,Head_MessageID,TransportContractDocument_ID,\n" +
				"  AssociatedTransportDocument_ID,FunctionCode,ACCURATE_IMONO,ACCURATE_VSLNAME,BIZ_STAGE_NAME,BIZ_STATUS,SENDER_CODE,\n" +
				"  if(EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO\n" +
				"from (select * from Mt1101JoinMt9999WithDimTB where TransportEquipment is not null) as tempTB4 cross join unnest(TransportEquipment) as TransportEquipment(EquipmentIdentification,CharacteristicCode,SupplierPartyTypeCode,FullnessCode,SealID)\n" +
				"union all \n" +
				"select msgId,GID,VSL_IMO_NO,VSL_NAME,VOYAGE,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_TIME, \n" +
				"  BIZ_STATUS_CODE,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE,Head_MessageID,TransportContractDocument_ID,\n" +
				"  AssociatedTransportDocument_ID,FunctionCode,ACCURATE_IMONO,ACCURATE_VSLNAME,BIZ_STAGE_NAME,BIZ_STATUS,SENDER_CODE,\n" +
				"  'N/A' as CTNR_NO\n" +
				"from Mt1101JoinMt9999WithDimTB where TransportEquipment is null");
		
		Table Mt1101JoinMt9999WithDim_ctnrTB = tEnv.sqlQuery("select * from Mt1101JoinMt9999WithDim_ctnrTB");
		tEnv.toAppendStream(Mt1101JoinMt9999WithDim_ctnrTB, Row.class).print();
//		env.execute();
		
		// TODO 结果表：箱单关系表
		tEnv.executeSql("" +
				"create view blctnrTB as \n" +
				"select VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"  if(AssociatedTransportDocument_ID = 'N/A', TransportContractDocument_ID, AssociatedTransportDocument_ID) as BL_NO,\n" +
				"  if(AssociatedTransportDocument_ID = 'N/A', AssociatedTransportDocument_ID, TransportContractDocument_ID) as MASTER_BL_NO,\n" +
				"  CTNR_NO,I_E_MARK,Head_MessageID as MESSAGE_ID,SENDER_CODE,\n" +
				"  if(CTNR_NO <> 'N/A', '0', '1') as BULK_FLAG,\n" +
				"  BIZ_TIME as RSP_CREATE_TIME,LASTUPDATEDDT,ISDELETED,\n" +
				"  FunctionCode\n" +
				"from Mt1101JoinMt9999WithDim_ctnrTB");
		
		Table blctnrTB = tEnv.sqlQuery("select * from blctnrTB");
		tEnv.toAppendStream(blctnrTB, Row.class).print();
//		env.execute();
		
		// TODO 结果表：原始舱单D6.1，I_cusDecl_mt1101提单状态表
		// TODO 结果表：分拨舱单D6a.3，I_cusDecl_mt1101Sub提单状态表
		tEnv.executeSql("" +
				"create view D6_1_and_D6a_3_BillTB as\n" +
				"select msgId,GID,\n" +
				"  VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"  if(BIZ_STAGE_NO='D6.1',TransportContractDocument_ID, AssociatedTransportDocument_ID) as BL_NO,\n" +
				"  if(BIZ_STAGE_NO='D6.1',AssociatedTransportDocument_ID, TransportContractDocument_ID) as MASTER_BL_NO,\n" +
				"  I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE\n" +
				"from mt9999WB_standerImoName\n" +
				"where BIZ_STAGE_NO<>'N/A' and BIZ_STAGE_CODE<>'N/A'");
		
		Table D6_1_and_D6a_3_BillTB = tEnv.sqlQuery("select * from D6_1_and_D6a_3_BillTB");
		tEnv.toAppendStream(D6_1_and_D6a_3_BillTB, Row.class).print();
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
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
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
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") with (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'mt1101_test_bill',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO oracle sink表，箱
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
		
		// TODO kafka sink表,箱
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
				"  'topic' = 'mt1101_test_ctnr',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO Oracle 箱单关系表
		tEnv.executeSql("" +
				"CREATE TABLE oracle_track_biz_blctnr (\n" +
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
		
		// TODO SINK
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 原始舱单D6.1，I_cusDecl_mt1101箱状态表，insert时需要过滤掉箱号为N/A的
		// TODO sink oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_ctnr \n" +
				"select \n" +
				"  tempTB5.VSL_IMO_NO,tempTB5.VSL_NAME,tempTB5.VOYAGE,tempTB5.ACCURATE_IMONO,tempTB5.ACCURATE_VSLNAME,\n" +
				"  tempTB5.CTNR_NO,tempTB5.I_E_MARK,tempTB5.BIZ_STAGE_NO,tempTB5.BIZ_STAGE_CODE,tempTB5.BIZ_STAGE_NAME,\n" +
				"  tempTB5.BIZ_TIME,tempTB5.BIZ_STATUS_CODE,tempTB5.BIZ_STATUS,tempTB5.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
				"  tempTB5.ISDELETED,tempTB5.UUID,tempTB5.BIZ_STATUS_IFFECTIVE\n" +
				"from Mt1101JoinMt9999WithDim_ctnrTB as tempTB5 left join oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF tempTB5.LASTUPDATEDDT as ocd\n" +
				"on tempTB5.VSL_IMO_NO = ocd.VSL_IMO_NO\n" +
				"and tempTB5.VOYAGE = ocd.VOYAGE\n" +
				"and tempTB5.CTNR_NO = ocd.CTNR_NO\n" +
				"and tempTB5.BIZ_STAGE_NO = ocd.BIZ_STAGE_NO\n" +
				"where (ocd.BIZ_TIME is null or tempTB5.BIZ_TIME>ocd.BIZ_TIME)\n" +
				"and tempTB5.BIZ_TIME is not null and tempTB5.CTNR_NO <> 'N/A'\n" +
				"and tempTB5.AssociatedTransportDocument_ID = 'N/A'");
		
		// TODO 原始舱单D6.1，I_cusDecl_mt1101箱状态表，insert时需要过滤掉箱号为N/A的
		// TODO sink kafka
		statementSet.addInsertSql("" +
				"insert into kafka_ctn\n" +
				"select \n" +
				"  GID,'DATA_FLINK_FULL_FLINK_TRACING_MT1101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE, \n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from \n" +
				"(select \n" +
				"  tempTB6.GID,tempTB6.VSL_IMO_NO,tempTB6.VSL_NAME,tempTB6.VOYAGE,tempTB6.ACCURATE_IMONO,tempTB6.ACCURATE_VSLNAME,\n" +
				"  tempTB6.CTNR_NO,tempTB6.I_E_MARK,tempTB6.BIZ_STAGE_NO,tempTB6.BIZ_STAGE_CODE,tempTB6.BIZ_STAGE_NAME,\n" +
				"  tempTB6.BIZ_TIME,tempTB6.BIZ_STATUS_CODE,tempTB6.BIZ_STATUS,tempTB6.BIZ_STATUS_DESC,\n" +
				"  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,tempTB6.ISDELETED,tempTB6.UUID,tempTB6.BIZ_STATUS_IFFECTIVE\n" +
				"from Mt1101JoinMt9999WithDim_ctnrTB as tempTB6 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tempTB6.LASTUPDATEDDT as ospd1\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT1101'=ospd1.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd1.TABLE_NAME\n" +
				"where ospd1.ISCURRENT=1 and tempTB6.BIZ_TIME is not null\n" +
				"and tempTB6.CTNR_NO <> 'N/A'\n" +
				"and tempTB6.AssociatedTransportDocument_ID = 'N/A') as temp1");
		
		// TODO 原始舱单D6.1，I_cusDecl_mt1101提单状态表
		// TODO 分拨舱单D6a.3，I_cusDecl_mt1101Sub提单状态表
		// TODO SINK oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"select \n" +
				"  d1.VSL_IMO_NO,d1.VSL_NAME,d1.VOYAGE,d1.ACCURATE_IMONO,d1.ACCURATE_VSLNAME,\n" +
				"  d1.BL_NO,d1.MASTER_BL_NO,d1.I_E_MARK,d1.BIZ_STAGE_NO,d1.BIZ_STAGE_CODE,\n" +
				"  d1.BIZ_STAGE_NAME,d1.BIZ_TIME,d1.BIZ_STATUS_CODE,d1.BIZ_STATUS,\n" +
				"  d1.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,d1.ISDELETED,d1.UUID,d1.BIZ_STATUS_IFFECTIVE\n" +
				"from D6_1_and_D6a_3_BillTB as d1 left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF d1.LASTUPDATEDDT as obd\n" +
				"on d1.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
				"and d1.VOYAGE = obd.VOYAGE\n" +
				"and d1.BL_NO = obd.BL_NO\n" +
				"and d1.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
				"where (obd.BIZ_TIME is null or d1.BIZ_TIME>obd.BIZ_TIME)\n" +
				"and d1.BIZ_TIME is not null");
		
		// TODO 原始舱单D6.1，I_cusDecl_mt1101提单状态表
		// TODO 分拨舱单D6a.3，I_cusDecl_mt1101Sub提单状态表
		// TODO KAFKA
		statementSet.addInsertSql("" +
				"insert into kafka_bill\n" +
				"select \n" +
				"  GID,'DATA_FLINK_FULL_FLINK_TRACING_MT1101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE, \n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select\n" +
				"  d2.GID,d2.VSL_IMO_NO,d2.VSL_NAME,d2.VOYAGE,d2.ACCURATE_IMONO,d2.ACCURATE_VSLNAME,\n" +
				"  d2.BL_NO,d2.MASTER_BL_NO,d2.I_E_MARK,d2.BIZ_STAGE_NO,d2.BIZ_STAGE_CODE,d2.BIZ_STAGE_NAME,\n" +
				"  d2.BIZ_TIME,d2.BIZ_STATUS_CODE,d2.BIZ_STATUS,d2.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,d2.ISDELETED,d2.UUID,d2.BIZ_STATUS_IFFECTIVE\n" +
				"from D6_1_and_D6a_3_BillTB as d2 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF d2.LASTUPDATEDDT as ospd2\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT1101'=ospd2.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
				"where ospd2.ISCURRENT=1 and d2.BIZ_TIME is not null) as temp2");
		
//		statementSet.addInsertSql("" +
//				"");
//		statementSet.addInsertSql("" +
//				"");
		
		statementSet.execute();
	}
}
