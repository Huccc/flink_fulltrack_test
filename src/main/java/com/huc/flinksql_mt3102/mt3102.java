package com.huc.flinksql_mt3102;

import com.easipass.flink.table.function.udsf.JsonToRowInMt3102;
import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class mt3102 {
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
				"  LASTUPDATEDDT AS PROCTIME()  + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'mt3102_source',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt3102_source',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");

//		Table kafka_source_data_table = tEnv.sqlQuery("select * from kafka_source_data");
//		tEnv.toAppendStream(kafka_source_data_table, Row.class).print();
//		env.execute();
		
		// TODO kafka推送箱状态信息
		tEnv.executeSql("" +
				"create table kafka_track_biz_status_ctnr(\n" +
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
				"    CTNR_NO STRING,    I_E_MARK STRING,\n" +
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
				"  'topic' = 'mt3102_ctnr_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt3102_ctnr_test',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO kafka推送提单状态信息
		tEnv.executeSql("" +
				"create table kafka_track_biz_status_bill(\n" +
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
				"  'topic' = 'mt3102_bill_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt3102_bill_test',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO oracle 数据源 提单状态信息
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
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO oracle 数据源 箱状态信息
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
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO 维表redis
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
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO 维表oracle
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
				"    'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"    --'dialect-name' = 'Oracle',\n" +
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				"    --'lookup.cache.max-rows' = '100',\n" +
				"    --'lookup.cache.ttl' = '100',\n" +
				"    --'lookup.max-retries' = '3'\n" +
				")");
		
		// TODO 注册临时系统函数
		tEnv.createTemporarySystemFunction("JSON_TO_ROW_IN_MT3102", JsonToRowInMt3102.class);
		
		// TODO 筛选出mt3102报文并解析
		tEnv.executeSql("" +
				"create view mt3102TB as\n" +
				"select  msgId,\n" +
				"        JSON_TO_ROW_IN_MT3102(parseData) as pData,\n" +
				"        LASTUPDATEDDT\n" +
				"from kafka_source_data\n" +
				"WHERE msgType = 'message_data' AND bizId = 'MT3102'");
		
		Table mt3102TB_table = tEnv.sqlQuery("select * from mt3102TB");
		tEnv.toAppendStream(mt3102TB_table, Row.class).print();
//		env.execute();
		
		// TODO 获取3102的箱信息
		tEnv.executeSql("" +
				"create view mt3102ctnr as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    pData.Head.MessageID as Head_MessageID,\n" +
				"    pData.Head.FunctionCode as Head_FunctionCode,\n" +
				"    if(pData.Declaration.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,--航次\n" +
				"    if(pData.Declaration.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(pData.Declaration.BorderTransportMeans.Name <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.Name, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_Name,--船名\n" +
				"    TO_TIMESTAMP(substr(pData.Declaration.UnloadingLocation.ArrivalDate,0,8) || '000000.0', 'yyyyMMddHHmmss.S') as ArrivalDate,\n" +
				"    if(TransportEquipment.EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportEquipment.EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO\n" +
				"from mt3102TB cross join  unnest(mt3102TB.pData.Declaration.TransportEquipment) AS TransportEquipment(EquipmentIdentification,CharacteristicCode,FullnessCode,SealID)\n" +
				"where pData.Head.MessageID LIKE 'D%'");
		
		Table mt3102ctnr_table = tEnv.sqlQuery("select * from mt3102ctnr");
		tEnv.toAppendStream(mt3102ctnr_table, Row.class).print();
//		env.execute();
		
		// TODO 注册临时解析函数9999
		tEnv.createTemporarySystemFunction("mt9999_JSON_TO_ROW_IN_MT3102", JsonToRowInMt9999.class);
		
		// TODO 筛选9999的报文并解析
		tEnv.executeSql("" +
				"create view mt9999TB as\n" +
				"select  msgId,\n" +
				"        LASTUPDATEDDT,\n" +
				"        mt9999_JSON_TO_ROW_IN_MT3102(parseData) as p9Data\n" +
				"from kafka_source_data\n" +
				"WHERE msgType = 'message_data' AND bizId = 'MT9999'");

//		Table mt9999TB_table = tEnv.sqlQuery("select * from mt9999TB");
//		tEnv.toAppendStream(mt9999TB_table, Row.class).print();
//		env.execute();
		
		// TODO 获取9999的公共字段和提单、箱
		tEnv.executeSql("" +
				"create view mt9999common as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    p9Data.Head.MessageID as MessageID,\n" +
				"    p9Data.Head.FunctionCode as FunctionCode,\n" +
				"    p9Data.Head.MessageType as MessageType,\n" +
				"    p9Data.Head.SendTime as SendTime,\n" +
				"    p9Data.Response.BorderTransportMeans.ID as BorderTransportMeans_ID,\n" +
				"    p9Data.Response.BorderTransportMeans.JourneyID as BorderTransportMeans_JourneyID,\n" +
				"    p9Data.Response.ResponseType.Code as ResponseType_Code,--整个报文的回执类型\n" +
				"    p9Data.Response.ResponseType.Text as ResponseType_Text,--整个报文的回执类型\n" +
				"    p9Data.Response.Consignment as Consignment,--提单回执\n" +
				"    p9Data.Response.TransportEquipment as TransportEquipment--箱回执\n" +
				"from mt9999TB\n" +
				"where p9Data.Head.MessageType = 'MT3102'");

//		Table mt9999common_table = tEnv.sqlQuery("select * from mt9999common");
//		tEnv.toAppendStream(mt9999common_table, Row.class).print();
//		env.execute();
		
		// TODO 展开9999的3102回执01箱,已处理IMO、航次、箱号
		tEnv.executeSql("" +
				"create view mt9999ctnr as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    MessageID,\n" +
				"    FunctionCode,\n" +
				"    MessageType,\n" +
				"    SendTime,\n" +
				"    if(BorderTransportMeans_ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_ID, '[\\t\\n\\r]', ''))),'UN', ''), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(BorderTransportMeans_JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,--航次\n" +
				"    ResponseType_Code, ResponseType_Text,--整个报文的回执类型\n" +
				"    if(EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as EquipmentIdentification_ID,--箱号\n" +
				"    ResponseType.Code as ctnr_ResponseType_Code,--箱的回执类型\n" +
				"    if(ResponseType.Text <> '', ResponseType.Text, 'N/A') as ctnr_ResponseType_Text --箱的回执类型\n" +
				"from (select * from mt9999common where TransportEquipment is not null) as temp3\n" +
				"    cross join unnest(TransportEquipment) AS TransportEquipment(EquipmentIdentification,ResponseType)");
		
		Table mt9999ctnr_table = tEnv.sqlQuery("select * from mt9999ctnr");
		tEnv.toAppendStream(mt9999ctnr_table, Row.class).print();
//		env.execute();
		
		// TODO 疏港运抵（箱）
		tEnv.executeSql("" +
				"create view mt3102_ctn as\n" +
				"select\n" +
				"    mt9999ctnr.msgId,\n" +
				"    mt9999ctnr.BorderTransportMeans_ID as VSL_IMO_NO,--船舶IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as VSL_NAME,--船名\n" +
				"    mt9999ctnr.BorderTransportMeans_JourneyID as VOYAGE,--航次\n" +
				"    if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_IMONO,--标准IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_VSLNAME,--标准船名\n" +
				"    EquipmentIdentification_ID as CTNR_NO,--箱号\n" +
				"    'I' as I_E_MARK,--进出口\n" +
				"    'D7.3' as BIZ_STAGE_NO,--业务环节节点\n" +
				"    'I_ctnrDeconsolidation_mt3102' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    if(dim3.res <> '',dim3.res, 'N/A') as BIZ_STAGE_NAME,--业务环节节点名称\n" +
				"    TO_TIMESTAMP(concat(substr(mt9999ctnr.SendTime,1,14), '.', substr(mt9999ctnr.SendTime,15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME, --业务发生时间\n" +
				"    mt9999ctnr.ctnr_ResponseType_Code as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,--业务状态\n" +
				"    if(mt9999ctnr.ctnr_ResponseType_Text <> '', mt9999ctnr.ctnr_ResponseType_Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    mt9999ctnr.LASTUPDATEDDT, --最后处理时间\n" +
				"    if(mt9999ctnr.FunctionCode = '3', 1, 0) as ISDELETED, --文件功能\n" +
				"    uuid() as UUID,\n" +
				"    if(mt9999ctnr.ctnr_ResponseType_Code='01' and mt9999ctnr.ctnr_ResponseType_Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from mt9999ctnr\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999ctnr.BorderTransportMeans_ID) = dim1.key and 'VSL_NAME_EN' = dim1.hashkey --取船名和标准船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999ctnr.BorderTransportMeans_ID) = dim2.key and 'IMO_NO' = dim2.hashkey --取标准IMO\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D7.3&SUB_STAGE_CODE=I_ctnrDeconsolidation_mt3102' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999ctnr.ctnr_ResponseType_Code) = dim4.key and 'TYPE_NAME' = dim4.hashkey --业务状态\n" +
				"where mt9999ctnr.MessageID like 'SEA%'");
		
		Table mt3102_ctn_table = tEnv.sqlQuery("select * from mt3102_ctn");
		tEnv.toAppendStream(mt3102_ctn_table, Row.class).print();
//		env.execute();
		
		// TODO 分拨运抵（箱）
		tEnv.executeSql("" +
				"create view mt3102sub_ctn as\n" +
				"select\n" +
				"    mt9999ctnr.msgId,\n" +
				"    mt9999ctnr.BorderTransportMeans_ID as VSL_IMO_NO,--船舶IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as VSL_NAME,--船名\n" +
				"    mt9999ctnr.BorderTransportMeans_JourneyID as VOYAGE,--航次\n" +
				"    if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_IMONO,--标准IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_VSLNAME,--标准船名\n" +
				"    EquipmentIdentification_ID as CTNR_NO,--箱号\n" +
				"    'I' as I_E_MARK,--进出口\n" +
				"    'D8.1' as BIZ_STAGE_NO,--业务环节节点\n" +
				"    'I_ctnrDeconsolidation_mt3102sub' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    if(dim3.res <> '',dim3.res, 'N/A') as BIZ_STAGE_NAME,--业务环节节点名称\n" +
				"    TO_TIMESTAMP(concat(substr(mt9999ctnr.SendTime,1,14), '.', substr(mt9999ctnr.SendTime,15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME, --业务发生时间\n" +
				"    mt9999ctnr.ctnr_ResponseType_Code as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,--业务状态\n" +
				"    if(mt9999ctnr.ctnr_ResponseType_Text <> '', mt9999ctnr.ctnr_ResponseType_Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    mt9999ctnr.LASTUPDATEDDT, --最后处理时间\n" +
				"    if(mt9999ctnr.FunctionCode = '3', 1, 0) as ISDELETED, --文件功能\n" +
				"    uuid() as UUID,\n" +
				"    if(mt9999ctnr.ctnr_ResponseType_Code='01' and mt9999ctnr.ctnr_ResponseType_Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from mt9999ctnr\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999ctnr.BorderTransportMeans_ID) = dim1.key and 'VSL_NAME_EN' = dim1.hashkey --取船名和标准船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999ctnr.BorderTransportMeans_ID) = dim2.key and 'IMO_NO' = dim2.hashkey --取标准IMO\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D8.1&SUB_STAGE_CODE=I_ctnrDeconsolidation_mt3102sub' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999ctnr.ctnr_ResponseType_Code) = dim4.key and 'TYPE_NAME' = dim4.hashkey --业务状态\n" +
				"where mt9999ctnr.MessageID like 'D%'");
		
		Table mt3102sub_ctn_table = tEnv.sqlQuery("select * from mt3102sub_ctn");
		tEnv.toAppendStream(mt3102sub_ctn_table, Row.class).print();
//		env.execute();
		
		// TODO 分拨运抵（提单）
		tEnv.executeSql("" +
				"create view mt3102sub_bill as\n" +
				"select\n" +
				"    mt3102sub_ctn.msgId,\n" +
				"    mt3102sub_ctn.VSL_IMO_NO,\n" +
				"    mt3102sub_ctn.VSL_NAME,\n" +
				"    mt3102sub_ctn.VOYAGE,\n" +
				"    mt3102sub_ctn.ACCURATE_IMONO,\n" +
				"    mt3102sub_ctn.ACCURATE_VSLNAME,\n" +
				"    oBCd.BL_NO,\n" +
				"    oBCd.MASTER_BL_NO,\n" +
				"    mt3102sub_ctn.I_E_MARK,\n" +
				"    mt3102sub_ctn.BIZ_STAGE_NO, --业务环节节点\n" +
				"    mt3102sub_ctn.BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    mt3102sub_ctn.BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"    mt3102sub_ctn.BIZ_TIME,\n" +
				"    mt3102sub_ctn.BIZ_STATUS_CODE,\n" +
				"    mt3102sub_ctn.BIZ_STATUS,\n" +
				"    mt3102sub_ctn.BIZ_STATUS_DESC,\n" +
				"    mt3102sub_ctn.LASTUPDATEDDT,\n" +
				"    mt3102sub_ctn.ISDELETED,\n" +
				"    mt3102sub_ctn.UUID,\n" +
				"    mt3102sub_ctn.BIZ_STATUS_IFFECTIVE\n" +
				"from mt3102sub_ctn\n" +
				"    left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF mt3102sub_ctn.LASTUPDATEDDT as oBCd\n" +
				"        on  mt3102sub_ctn.VSL_IMO_NO = oBCd.VSL_IMO_NO\n" +
				"        and mt3102sub_ctn.VOYAGE = oBCd.VOYAGE\n" +
				"        and mt3102sub_ctn.CTNR_NO = oBCd.CTNR_NO\n" +
				"where oBCd.BL_NO is not null and oBCd.MASTER_BL_NO <> 'N/A'");
		
		Table mt3102sub_bill_table = tEnv.sqlQuery("select * from mt3102sub_bill");
		tEnv.toAppendStream(mt3102sub_bill_table, Row.class).print();
//		env.execute();
		
		// TODO 分拨入库（箱）
		tEnv.executeSql("" +
				"create view mt3102subarv_ctn as\n" +
				"select temp4.*,\n" +
				"  if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
				"  if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS, --业务状态\n" +
				"  uuid() as UUID,\n" +
				"  1 as BIZ_STATUS_IFFECTIVE\n" +
				"from\n" +
				"(\n" +
				"    select\n" +
				"        mt3102ctnr.msgId,\n" +
				"        mt3102ctnr.BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO\n" +
				"        mt3102ctnr.BorderTransportMeans_Name as VSL_NAME, --船名\n" +
				"        mt3102ctnr.BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"        mt3102ctnr.CTNR_NO as CTNR_NO, --箱号\n" +
				"        'I' as I_E_MARK, --进出口\n" +
				"        'D8.1a' as BIZ_STAGE_NO, --业务环节节点\n" +
				"        'I_ctnrDeconsolidation_mt3102subarv' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"        mt3102ctnr.ArrivalDate as BIZ_TIME, --业务发生时间\n" +
				"        '1' as BIZ_STATUS_CODE, --业务状态代码\n" +
				"        'N/A' as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"        mt3102ctnr.LASTUPDATEDDT, --最后处理时间\n" +
				"        if(mt3102ctnr.Head_FunctionCode='3',1,0) as ISDELETED --是否删除\n" +
				"    from mt3102ctnr\n" +
				") as temp4\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO匹配,找imo\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO匹配,找船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名匹配,找imo\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名匹配,找船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',temp4.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',temp4.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=arrival_status&TYPE_CODE=',temp4.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table mt3102subarv_ctn_table = tEnv.sqlQuery("select * from mt3102subarv_ctn");
		tEnv.toAppendStream(mt3102subarv_ctn_table, Row.class).print();
//		env.execute();
		
		// TODO 分拨入库（提单）
		tEnv.executeSql("" +
				"create view mt3102subarv_bill as\n" +
				"select\n" +
				"    mt3102subarv_ctn.msgId,\n" +
				"    mt3102subarv_ctn.VSL_IMO_NO,\n" +
				"    mt3102subarv_ctn.VSL_NAME,\n" +
				"    mt3102subarv_ctn.VOYAGE,\n" +
				"    mt3102subarv_ctn.ACCURATE_IMONO,\n" +
				"    mt3102subarv_ctn.ACCURATE_VSLNAME,\n" +
				"    oBCd.BL_NO,\n" +
				"    oBCd.MASTER_BL_NO,\n" +
				"    mt3102subarv_ctn.I_E_MARK,\n" +
				"    mt3102subarv_ctn.BIZ_STAGE_NO, --业务环节节点\n" +
				"    mt3102subarv_ctn.BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    mt3102subarv_ctn.BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"    mt3102subarv_ctn.BIZ_TIME,\n" +
				"    mt3102subarv_ctn.BIZ_STATUS_CODE,\n" +
				"    mt3102subarv_ctn.BIZ_STATUS,\n" +
				"    mt3102subarv_ctn.BIZ_STATUS_DESC,\n" +
				"    mt3102subarv_ctn.LASTUPDATEDDT,\n" +
				"    mt3102subarv_ctn.ISDELETED,\n" +
				"    mt3102subarv_ctn.UUID,\n" +
				"    mt3102subarv_ctn.BIZ_STATUS_IFFECTIVE\n" +
				"from mt3102subarv_ctn\n" +
				"    left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF mt3102subarv_ctn.LASTUPDATEDDT as oBCd\n" +
				"        on  mt3102subarv_ctn.VSL_IMO_NO = oBCd.VSL_IMO_NO\n" +
				"        and mt3102subarv_ctn.VOYAGE = oBCd.VOYAGE\n" +
				"        and mt3102subarv_ctn.CTNR_NO = oBCd.CTNR_NO\n" +
				"where oBCd.BL_NO is not null and oBCd.MASTER_BL_NO <> 'N/A'");
		
		Table mt3102subarv_bill_table = tEnv.sqlQuery("select * from mt3102subarv_bill");
		tEnv.toAppendStream(mt3102subarv_bill_table, Row.class).print();
//		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO 疏港运抵（箱）,oracle
		statementSet.addInsertSql("" +
				"INSERT INTO oracle_track_biz_status_ctnr (VSL_IMO_NO,\n" +
				"                                          VSL_NAME,\n" +
				"                                          VOYAGE,\n" +
				"                                          ACCURATE_IMONO,\n" +
				"                                          ACCURATE_VSLNAME,\n" +
				"                                          CTNR_NO,\n" +
				"                                          I_E_MARK,\n" +
				"                                          BIZ_STAGE_NO,\n" +
				"                                          BIZ_STAGE_CODE,\n" +
				"                                          BIZ_STAGE_NAME,\n" +
				"                                          BIZ_TIME,\n" +
				"                                          BIZ_STATUS_CODE,\n" +
				"                                          BIZ_STATUS,\n" +
				"                                          BIZ_STATUS_DESC,\n" +
				"                                          LASTUPDATEDDT,\n" +
				"                                          ISDELETED,\n" +
				"                                          UUID,\n" +
				"                                          BIZ_STATUS_IFFECTIVE)\n" +
				"   SELECT M.VSL_IMO_NO,\n" +
				"          M.VSL_NAME,\n" +
				"          M.VOYAGE,\n" +
				"          M.ACCURATE_IMONO,\n" +
				"          M.ACCURATE_VSLNAME,\n" +
				"          M.CTNR_NO,\n" +
				"          M.I_E_MARK,\n" +
				"          M.BIZ_STAGE_NO,\n" +
				"          M.BIZ_STAGE_CODE,\n" +
				"          M.BIZ_STAGE_NAME,\n" +
				"          M.BIZ_TIME,\n" +
				"          M.BIZ_STATUS_CODE,\n" +
				"          M.BIZ_STATUS,\n" +
				"          M.BIZ_STATUS_DESC,\n" +
				"          M.LASTUPDATEDDT,\n" +
				"          M.ISDELETED,\n" +
				"          M.UUID,\n" +
				"          M.BIZ_STATUS_IFFECTIVE\n" +
				"     FROM mt3102_ctn AS M\n" +
				"    LEFT JOIN oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS T\n" +
				"        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
				"        AND M.VOYAGE = T.VOYAGE\n" +
				"        AND M.CTNR_NO = T.CTNR_NO\n" +
				"        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
				"WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
				"AND M.BIZ_TIME IS NOT NULL");
		
		// TODO 疏港运抵（箱）,kafka
		statementSet.addInsertSql("" +
				"INSERT INTO kafka_track_biz_status_ctnr (GID,\n" +
				"                                         APP_NAME,\n" +
				"                                         TABLE_NAME,\n" +
				"                                         SUBSCRIBE_TYPE,\n" +
				"                                         DATA)\n" +
				"   SELECT UUID AS GID,\n" +
				"          'DATA_FLINK_FULL_FLINK_TRACING_MT3102' AS APP_NAME,\n" +
				"          'DM.TRACK_BIZ_STATUS_CTNR' AS TABLE_NAME,\n" +
				"          'I' AS SUBSCRIBE_TYPE,\n" +
				"          ROW (VSL_IMO_NO,\n" +
				"               VSL_NAME,\n" +
				"               VOYAGE,\n" +
				"               ACCURATE_IMONO,\n" +
				"               ACCURATE_VSLNAME,\n" +
				"               CTNR_NO,\n" +
				"               I_E_MARK,\n" +
				"               BIZ_STAGE_NO,\n" +
				"               BIZ_STAGE_CODE,\n" +
				"               BIZ_STAGE_NAME,\n" +
				"               BIZ_TIME,\n" +
				"               BIZ_STATUS_CODE,\n" +
				"               BIZ_STATUS,\n" +
				"               BIZ_STATUS_DESC,\n" +
				"               LASTUPDATEDDT,\n" +
				"               ISDELETED,\n" +
				"               BIZ_STATUS_IFFECTIVE)\n" +
				"             AS DATA\n" +
				"     FROM (SELECT M.UUID,\n" +
				"                  M.VSL_IMO_NO,\n" +
				"                  M.VSL_NAME,\n" +
				"                  M.VOYAGE,\n" +
				"                  M.ACCURATE_IMONO,\n" +
				"                  M.ACCURATE_VSLNAME,\n" +
				"                  M.CTNR_NO,\n" +
				"                  M.I_E_MARK,\n" +
				"                  M.BIZ_STAGE_NO,\n" +
				"                  M.BIZ_STAGE_CODE,\n" +
				"                  M.BIZ_STAGE_NAME,\n" +
				"                  M.BIZ_TIME,\n" +
				"                  M.BIZ_STATUS_CODE,\n" +
				"                  M.BIZ_STATUS,\n" +
				"                  M.BIZ_STATUS_DESC,\n" +
				"                  M.LASTUPDATEDDT,\n" +
				"                  M.ISDELETED,\n" +
				"                  M.BIZ_STATUS_IFFECTIVE\n" +
				"             FROM mt3102_ctn AS M\n" +
				"            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS P\n" +
				"                  ON 'DATA_FLINK_FULL_FLINK_TRACING_MT3102'=P.APP_NAME\n" +
				"                  AND 'DM.TRACK_BIZ_STATUS_CTNR'=P.TABLE_NAME\n" +
				"WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");
		
		// TODO 分拨运抵（箱）,oracle
		statementSet.addInsertSql("" +
				"INSERT INTO oracle_track_biz_status_ctnr (VSL_IMO_NO,\n" +
				"                                          VSL_NAME,\n" +
				"                                          VOYAGE,\n" +
				"                                          ACCURATE_IMONO,\n" +
				"                                          ACCURATE_VSLNAME,\n" +
				"                                          CTNR_NO,\n" +
				"                                          I_E_MARK,\n" +
				"                                          BIZ_STAGE_NO,\n" +
				"                                          BIZ_STAGE_CODE,\n" +
				"                                          BIZ_STAGE_NAME,\n" +
				"                                          BIZ_TIME,\n" +
				"                                          BIZ_STATUS_CODE,\n" +
				"                                          BIZ_STATUS,\n" +
				"                                          BIZ_STATUS_DESC,\n" +
				"                                          LASTUPDATEDDT,\n" +
				"                                          ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"   SELECT M.VSL_IMO_NO,\n" +
				"          M.VSL_NAME,\n" +
				"          M.VOYAGE,\n" +
				"          M.ACCURATE_IMONO,\n" +
				"          M.ACCURATE_VSLNAME,\n" +
				"          M.CTNR_NO,\n" +
				"          M.I_E_MARK,\n" +
				"          M.BIZ_STAGE_NO,\n" +
				"          M.BIZ_STAGE_CODE,\n" +
				"          M.BIZ_STAGE_NAME,\n" +
				"          M.BIZ_TIME,\n" +
				"          M.BIZ_STATUS_CODE,\n" +
				"          M.BIZ_STATUS,\n" +
				"          M.BIZ_STATUS_DESC,\n" +
				"          M.LASTUPDATEDDT,\n" +
				"          M.ISDELETED,M.UUID,M.BIZ_STATUS_IFFECTIVE\n" +
				"     FROM mt3102sub_ctn AS M\n" +
				"    LEFT JOIN oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS T\n" +
				"        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
				"        AND M.VOYAGE = T.VOYAGE\n" +
				"        AND M.CTNR_NO = T.CTNR_NO\n" +
				"        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
				"WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
				"AND M.BIZ_TIME IS NOT NULL");
		
		// TODO 分拨运抵（箱）,kafka
		statementSet.addInsertSql("" +
				"INSERT INTO kafka_track_biz_status_ctnr (GID,\n" +
				"                                         APP_NAME,\n" +
				"                                         TABLE_NAME,\n" +
				"                                         SUBSCRIBE_TYPE,\n" +
				"                                         DATA)\n" +
				"   SELECT UUID AS GID,\n" +
				"          'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB' AS APP_NAME,\n" +
				"          'DM.TRACK_BIZ_STATUS_CTNR' AS TABLE_NAME,\n" +
				"          'I' AS SUBSCRIBE_TYPE,\n" +
				"          ROW (VSL_IMO_NO,\n" +
				"               VSL_NAME,\n" +
				"               VOYAGE,\n" +
				"               ACCURATE_IMONO,\n" +
				"               ACCURATE_VSLNAME,\n" +
				"               CTNR_NO,\n" +
				"               I_E_MARK,\n" +
				"               BIZ_STAGE_NO,\n" +
				"               BIZ_STAGE_CODE,\n" +
				"               BIZ_STAGE_NAME,\n" +
				"               BIZ_TIME,\n" +
				"               BIZ_STATUS_CODE,\n" +
				"               BIZ_STATUS,\n" +
				"               BIZ_STATUS_DESC,\n" +
				"               LASTUPDATEDDT,\n" +
				"               ISDELETED,BIZ_STATUS_IFFECTIVE)\n" +
				"             AS DATA\n" +
				"     FROM (SELECT M.UUID,\n" +
				"                  M.VSL_IMO_NO,\n" +
				"                  M.VSL_NAME,\n" +
				"                  M.VOYAGE,\n" +
				"                  M.ACCURATE_IMONO,\n" +
				"                  M.ACCURATE_VSLNAME,\n" +
				"                  M.CTNR_NO,\n" +
				"                  M.I_E_MARK,\n" +
				"                  M.BIZ_STAGE_NO,\n" +
				"                  M.BIZ_STAGE_CODE,\n" +
				"                  M.BIZ_STAGE_NAME,\n" +
				"                  M.BIZ_TIME,\n" +
				"                  M.BIZ_STATUS_CODE,\n" +
				"                  M.BIZ_STATUS,\n" +
				"                  M.BIZ_STATUS_DESC,\n" +
				"                  M.LASTUPDATEDDT,\n" +
				"                  M.ISDELETED,M.BIZ_STATUS_IFFECTIVE\n" +
				"             FROM mt3102sub_ctn AS M\n" +
				"            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS P\n" +
				"                  ON 'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB'=P.APP_NAME\n" +
				"                  AND 'DM.TRACK_BIZ_STATUS_CTNR'=P.TABLE_NAME\n" +
				"WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");
		
		// TODO 分拨运抵（提单）,oracle
		statementSet.addInsertSql("" +
				"INSERT INTO oracle_track_biz_status_bill (VSL_IMO_NO,\n" +
				"                                          VSL_NAME,\n" +
				"                                          VOYAGE,\n" +
				"                                          ACCURATE_IMONO,\n" +
				"                                          ACCURATE_VSLNAME,\n" +
				"                                          BL_NO,\n" +
				"                                          MASTER_BL_NO,\n" +
				"                                          I_E_MARK,\n" +
				"                                          BIZ_STAGE_NO,\n" +
				"                                          BIZ_STAGE_CODE,\n" +
				"                                          BIZ_STAGE_NAME,\n" +
				"                                          BIZ_TIME,\n" +
				"                                          BIZ_STATUS_CODE,\n" +
				"                                          BIZ_STATUS,\n" +
				"                                          BIZ_STATUS_DESC,\n" +
				"                                          LASTUPDATEDDT,\n" +
				"                                          ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"   SELECT M.VSL_IMO_NO,\n" +
				"          M.VSL_NAME,\n" +
				"          M.VOYAGE,\n" +
				"          M.ACCURATE_IMONO,\n" +
				"          M.ACCURATE_VSLNAME,\n" +
				"          M.BL_NO,\n" +
				"          M.MASTER_BL_NO,\n" +
				"          M.I_E_MARK,\n" +
				"          M.BIZ_STAGE_NO,\n" +
				"          M.BIZ_STAGE_CODE,\n" +
				"          M.BIZ_STAGE_NAME,\n" +
				"          M.BIZ_TIME,\n" +
				"          M.BIZ_STATUS_CODE,\n" +
				"          M.BIZ_STATUS,\n" +
				"          M.BIZ_STATUS_DESC,\n" +
				"          M.LASTUPDATEDDT,\n" +
				"          M.ISDELETED,M.UUID,M.BIZ_STATUS_IFFECTIVE\n" +
				"     FROM mt3102sub_bill AS M\n" +
				"    LEFT JOIN oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS T\n" +
				"        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
				"        AND M.VOYAGE = T.VOYAGE\n" +
				"        AND M.BL_NO = T.BL_NO\n" +
				"        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
				"WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
				"AND M.BIZ_TIME IS NOT NULL");
		
		// TODO 分拨运抵（提单）,kafka
		statementSet.addInsertSql("" +
				"INSERT INTO kafka_track_biz_status_bill (GID,\n" +
				"                                         APP_NAME,\n" +
				"                                         TABLE_NAME,\n" +
				"                                         SUBSCRIBE_TYPE,\n" +
				"                                         DATA)\n" +
				"   SELECT UUID AS GID,\n" +
				"          'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB' AS APP_NAME,\n" +
				"          'DM.TRACK_BIZ_STATUS_BILL' AS TABLE_NAME,\n" +
				"          'I' AS SUBSCRIBE_TYPE,\n" +
				"          ROW (VSL_IMO_NO,\n" +
				"               VSL_NAME,\n" +
				"               VOYAGE,\n" +
				"               ACCURATE_IMONO,\n" +
				"               ACCURATE_VSLNAME,\n" +
				"               BL_NO,\n" +
				"               MASTER_BL_NO,\n" +
				"               I_E_MARK,\n" +
				"               BIZ_STAGE_NO,\n" +
				"               BIZ_STAGE_CODE,\n" +
				"               BIZ_STAGE_NAME,\n" +
				"               BIZ_TIME,\n" +
				"               BIZ_STATUS_CODE,\n" +
				"               BIZ_STATUS,\n" +
				"               BIZ_STATUS_DESC,\n" +
				"               LASTUPDATEDDT,\n" +
				"               ISDELETED,BIZ_STATUS_IFFECTIVE)\n" +
				"             AS DATA\n" +
				"     FROM (SELECT M.UUID,\n" +
				"                  M.VSL_IMO_NO,\n" +
				"                  M.VSL_NAME,\n" +
				"                  M.VOYAGE,\n" +
				"                  M.ACCURATE_IMONO,\n" +
				"                  M.ACCURATE_VSLNAME,\n" +
				"                  M.BL_NO,\n" +
				"                  M.MASTER_BL_NO,\n" +
				"                  M.I_E_MARK,\n" +
				"                  M.BIZ_STAGE_NO,\n" +
				"                  M.BIZ_STAGE_CODE,\n" +
				"                  M.BIZ_STAGE_NAME,\n" +
				"                  M.BIZ_TIME,\n" +
				"                  M.BIZ_STATUS_CODE,\n" +
				"                  M.BIZ_STATUS,\n" +
				"                  M.BIZ_STATUS_DESC,\n" +
				"                  M.LASTUPDATEDDT,\n" +
				"                  M.ISDELETED,M.BIZ_STATUS_IFFECTIVE\n" +
				"             FROM mt3102sub_bill AS M\n" +
				"            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS P\n" +
				"                  ON 'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB'=P.APP_NAME\n" +
				"                  AND 'DM.TRACK_BIZ_STATUS_BILL'=P.TABLE_NAME\n" +
				"WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");
		
		// TODO 分拨入库（箱）,oracle
		statementSet.addInsertSql("" +
				"INSERT INTO oracle_track_biz_status_ctnr (VSL_IMO_NO,\n" +
				"                                          VSL_NAME,\n" +
				"                                          VOYAGE,\n" +
				"                                          ACCURATE_IMONO,\n" +
				"                                          ACCURATE_VSLNAME,\n" +
				"                                          CTNR_NO,\n" +
				"                                          I_E_MARK,\n" +
				"                                          BIZ_STAGE_NO,\n" +
				"                                          BIZ_STAGE_CODE,\n" +
				"                                          BIZ_STAGE_NAME,\n" +
				"                                          BIZ_TIME,\n" +
				"                                          BIZ_STATUS_CODE,\n" +
				"                                          BIZ_STATUS,\n" +
				"                                          BIZ_STATUS_DESC,\n" +
				"                                          LASTUPDATEDDT,\n" +
				"                                          ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"   SELECT M.VSL_IMO_NO,\n" +
				"          M.VSL_NAME,\n" +
				"          M.VOYAGE,\n" +
				"          M.ACCURATE_IMONO,\n" +
				"          M.ACCURATE_VSLNAME,\n" +
				"          M.CTNR_NO,\n" +
				"          M.I_E_MARK,\n" +
				"          M.BIZ_STAGE_NO,\n" +
				"          M.BIZ_STAGE_CODE,\n" +
				"          M.BIZ_STAGE_NAME,\n" +
				"          M.BIZ_TIME,\n" +
				"          M.BIZ_STATUS_CODE,\n" +
				"          M.BIZ_STATUS,\n" +
				"          M.BIZ_STATUS_DESC,\n" +
				"          M.LASTUPDATEDDT,\n" +
				"          M.ISDELETED,M.UUID,M.BIZ_STATUS_IFFECTIVE\n" +
				"     FROM mt3102subarv_ctn AS M\n" +
				"    LEFT JOIN oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS T\n" +
				"        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
				"        AND M.VOYAGE = T.VOYAGE\n" +
				"        AND M.CTNR_NO = T.CTNR_NO\n" +
				"        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
				"WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
				"AND M.BIZ_TIME IS NOT NULL");
		
		// TODO 分拨入库（箱）,kafka
		statementSet.addInsertSql("" +
				"INSERT INTO kafka_track_biz_status_ctnr (GID,\n" +
				"                                         APP_NAME,\n" +
				"                                         TABLE_NAME,\n" +
				"                                         SUBSCRIBE_TYPE,\n" +
				"                                         DATA)\n" +
				"   SELECT UUID AS GID,\n" +
				"          'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB_ARV' AS APP_NAME,\n" +
				"          'DM.TRACK_BIZ_STATUS_CTNR' AS TABLE_NAME,\n" +
				"          'I' AS SUBSCRIBE_TYPE,\n" +
				"          ROW (VSL_IMO_NO,\n" +
				"               VSL_NAME,\n" +
				"               VOYAGE,\n" +
				"               ACCURATE_IMONO,\n" +
				"               ACCURATE_VSLNAME,\n" +
				"               CTNR_NO,\n" +
				"               I_E_MARK,\n" +
				"               BIZ_STAGE_NO,\n" +
				"               BIZ_STAGE_CODE,\n" +
				"               BIZ_STAGE_NAME,\n" +
				"               BIZ_TIME,\n" +
				"               BIZ_STATUS_CODE,\n" +
				"               BIZ_STATUS,\n" +
				"               BIZ_STATUS_DESC,\n" +
				"               LASTUPDATEDDT,\n" +
				"               ISDELETED,BIZ_STATUS_IFFECTIVE)\n" +
				"             AS DATA\n" +
				"     FROM (SELECT M.UUID,\n" +
				"                  M.VSL_IMO_NO,\n" +
				"                  M.VSL_NAME,\n" +
				"                  M.VOYAGE,\n" +
				"                  M.ACCURATE_IMONO,\n" +
				"                  M.ACCURATE_VSLNAME,\n" +
				"                  M.CTNR_NO,\n" +
				"                  M.I_E_MARK,\n" +
				"                  M.BIZ_STAGE_NO,\n" +
				"                  M.BIZ_STAGE_CODE,\n" +
				"                  M.BIZ_STAGE_NAME,\n" +
				"                  M.BIZ_TIME,\n" +
				"                  M.BIZ_STATUS_CODE,\n" +
				"                  M.BIZ_STATUS,\n" +
				"                  M.BIZ_STATUS_DESC,\n" +
				"                  M.LASTUPDATEDDT,\n" +
				"                  M.ISDELETED,M.BIZ_STATUS_IFFECTIVE\n" +
				"             FROM mt3102subarv_ctn AS M\n" +
				"            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS P\n" +
				"                  ON 'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB_ARV'=P.APP_NAME\n" +
				"                  AND 'DM.TRACK_BIZ_STATUS_CTNR'=P.TABLE_NAME\n" +
				"WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");
		
		// TODO 分拨入库（提单）,oracle
		statementSet.addInsertSql("" +
				"INSERT INTO oracle_track_biz_status_bill (VSL_IMO_NO,\n" +
				"                                          VSL_NAME,\n" +
				"                                          VOYAGE,\n" +
				"                                          ACCURATE_IMONO,\n" +
				"                                          ACCURATE_VSLNAME,\n" +
				"                                          BL_NO,\n" +
				"                                          MASTER_BL_NO,\n" +
				"                                          I_E_MARK,\n" +
				"                                          BIZ_STAGE_NO,\n" +
				"                                          BIZ_STAGE_CODE,\n" +
				"                                          BIZ_STAGE_NAME,\n" +
				"                                          BIZ_TIME,\n" +
				"                                          BIZ_STATUS_CODE,\n" +
				"                                          BIZ_STATUS,\n" +
				"                                          BIZ_STATUS_DESC,\n" +
				"                                          LASTUPDATEDDT,\n" +
				"                                          ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"   SELECT M.VSL_IMO_NO,\n" +
				"          M.VSL_NAME,\n" +
				"          M.VOYAGE,\n" +
				"          M.ACCURATE_IMONO,\n" +
				"          M.ACCURATE_VSLNAME,\n" +
				"          M.BL_NO,\n" +
				"          M.MASTER_BL_NO,\n" +
				"          M.I_E_MARK,\n" +
				"          M.BIZ_STAGE_NO,\n" +
				"          M.BIZ_STAGE_CODE,\n" +
				"          M.BIZ_STAGE_NAME,\n" +
				"          M.BIZ_TIME,\n" +
				"          M.BIZ_STATUS_CODE,\n" +
				"          M.BIZ_STATUS,\n" +
				"          M.BIZ_STATUS_DESC,\n" +
				"          M.LASTUPDATEDDT,\n" +
				"          M.ISDELETED,M.UUID,M.BIZ_STATUS_IFFECTIVE\n" +
				"     FROM mt3102subarv_bill AS M\n" +
				"    LEFT JOIN oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS T\n" +
				"        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
				"        AND M.VOYAGE = T.VOYAGE\n" +
				"        AND M.BL_NO = T.BL_NO\n" +
				"        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
				"WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
				"AND M.BIZ_TIME IS NOT NULL");
		
		// TODO 分拨入库（提单）,kafka
		statementSet.addInsertSql("" +
				"INSERT INTO kafka_track_biz_status_bill (GID,\n" +
				"                                         APP_NAME,\n" +
				"                                         TABLE_NAME,\n" +
				"                                         SUBSCRIBE_TYPE,\n" +
				"                                         DATA)\n" +
				"   SELECT UUID AS GID,\n" +
				"          'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB_ARV' AS APP_NAME,\n" +
				"          'DM.TRACK_BIZ_STATUS_BILL' AS TABLE_NAME,\n" +
				"          'I' AS SUBSCRIBE_TYPE,\n" +
				"          ROW (VSL_IMO_NO,\n" +
				"               VSL_NAME,\n" +
				"               VOYAGE,\n" +
				"               ACCURATE_IMONO,\n" +
				"               ACCURATE_VSLNAME,\n" +
				"               BL_NO,\n" +
				"               MASTER_BL_NO,\n" +
				"               I_E_MARK,\n" +
				"               BIZ_STAGE_NO,\n" +
				"               BIZ_STAGE_CODE,\n" +
				"               BIZ_STAGE_NAME,\n" +
				"               BIZ_TIME,\n" +
				"               BIZ_STATUS_CODE,\n" +
				"               BIZ_STATUS,\n" +
				"               BIZ_STATUS_DESC,\n" +
				"               LASTUPDATEDDT,\n" +
				"               ISDELETED,BIZ_STATUS_IFFECTIVE)\n" +
				"             AS DATA\n" +
				"     FROM (SELECT M.UUID,\n" +
				"                  M.VSL_IMO_NO,\n" +
				"                  M.VSL_NAME,\n" +
				"                  M.VOYAGE,\n" +
				"                  M.ACCURATE_IMONO,\n" +
				"                  M.ACCURATE_VSLNAME,\n" +
				"                  M.BL_NO,\n" +
				"                  M.MASTER_BL_NO,\n" +
				"                  M.I_E_MARK,\n" +
				"                  M.BIZ_STAGE_NO,\n" +
				"                  M.BIZ_STAGE_CODE,\n" +
				"                  M.BIZ_STAGE_NAME,\n" +
				"                  M.BIZ_TIME,\n" +
				"                  M.BIZ_STATUS_CODE,\n" +
				"                  M.BIZ_STATUS,\n" +
				"                  M.BIZ_STATUS_DESC,\n" +
				"                  M.LASTUPDATEDDT,\n" +
				"                  M.ISDELETED,M.BIZ_STATUS_IFFECTIVE\n" +
				"             FROM mt3102subarv_bill AS M\n" +
				"            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.LASTUPDATEDDT AS P\n" +
				"                  ON 'DATA_FLINK_FULL_FLINK_TRACING_MT3102SUB_ARV'=P.APP_NAME\n" +
				"                  AND 'DM.TRACK_BIZ_STATUS_BILL'=P.TABLE_NAME\n" +
				"WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");
		
		statementSet.execute();
	}
}
