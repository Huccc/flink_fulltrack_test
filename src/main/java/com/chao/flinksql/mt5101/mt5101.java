package com.chao.flinksql.mt5101;

import com.easipass.flink.table.function.udsf.JsonToRowInMT5101;
import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class mt5101 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
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
				"  LASTUPDATEDDT AS PROCTIME()  + INTERVAL '8' HOUR\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'data-xpq-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'data-xpq-msg-parse-result',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'latest-offset'\n" +
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
				"  'topic' = 'mt5101_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt5101_test',\n" +
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
				"  'topic' = 'mt5101_test',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
				"  'properties.group.id' = 'mt5101_test',\n" +
				"  'format' = 'json'\n" +
				")");
		
		// TODO oracle 数据源
		// TODO 提单状态信息
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
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO 箱状态信息
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
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO 维表 维表oracle
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
				"    'username' = 'xqwu',\n" +
				"    'password' = 'easipass'\n" +
				"    --'lookup.cache.max-rows' = '100',\n" +
				"    --'lookup.cache.ttl' = '100',\n" +
				"    --'lookup.max-retries' = '3'\n" +
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
		
		// TODO Oracle箱单关系表(维表),与MT9999箱回执关联得到提单关系表
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
		
		// TODO 注册解析函数
		tEnv.createTemporarySystemFunction("JSON_TO_ROW_IN_MT5101", JsonToRowInMT5101.class);
		
		// TODO 筛选出5101的报文，并解析
		tEnv.executeSql("" +
				"create view mt5101TB as\n" +
				"select  msgId,\n" +
				"        JSON_TO_ROW_IN_MT5101(parseData) as pData,\n" +
				"        LASTUPDATEDDT\n" +
				"from kafka_source_data\n" +
				"WHERE msgType = 'message_data' AND bizId = 'MT5101'");

//		Table mt5101TB_table = tEnv.sqlQuery("select * from mt5101TB");
//		tEnv.toAppendStream(mt5101TB_table, Row.class).print();
//		env.execute();
		
		// TODO 获取mt5101的公共字段
		tEnv.executeSql("" +
				"create view mt5101common as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    pData.Head.MessageID as Head_MessageID,\n" +
				"    pData.Head.FunctionCode as Head_FunctionCode,\n" +
				"    pData.Head.SendTime as Head_SendTime,\n" +
				"    pData.Declaration.DeclarationOfficeID as DeclarationOfficeID,\n" +
				"    if(pData.Declaration.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,--航次\n" +
				"    pData.Declaration.BorderTransportMeans.TypeCode as BorderTransportMeans_TypeCode,\n" +
//				"    if(pData.Declaration.BorderTransportMeans.ID <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(pData.Declaration.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(pData.Declaration.BorderTransportMeans.Name <> '', UPPER(TRIM(REGEXP_REPLACE(pData.Declaration.BorderTransportMeans.Name, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_Name,--船名\n" +
				"    pData.Declaration.BorderTransportMeans.ActualDateTime as BorderTransportMeans_ActualDateTime,\n" +
				"    TO_TIMESTAMP(concat(substr(pData.Declaration.BorderTransportMeans.CompletedDateTime,1,14), '.', substr(pData.Declaration.BorderTransportMeans.CompletedDateTime,15)), 'yyyyMMddHHmmss.S') AS BorderTransportMeans_CompletedDateTime,\n" +
				"    pData.Declaration.BorderTransportMeans.UnloadingLocation.ID as BorderTransportMeans_UnloadingLocation_ID\n" +
				"from mt5101TB");
		
		Table mt5101common_table = tEnv.sqlQuery("select * from mt5101common");
		tEnv.toAppendStream(mt5101common_table, Row.class).print();
//		env.execute();
		
		// TODO 注册解析函数9999
		tEnv.createTemporarySystemFunction("mt9999_JSON_TO_ROW_IN_MT5101", JsonToRowInMt9999.class);
		
		// TODO 筛选9999的报文并解析
		tEnv.executeSql("" +
				"create view mt9999TB as\n" +
				"select  msgId,\n" +
				"        LASTUPDATEDDT,\n" +
				"        mt9999_JSON_TO_ROW_IN_MT5101(parseData) as p9Data\n" +
				"from kafka_source_data\n" +
				"WHERE msgType = 'message_data' AND bizId = 'MT9999'");
		
		// TODO 获取9999的公共字段和提单、箱
		tEnv.executeSql("" +
				"create view mt9999common as\n" +
				"select\n" +
				"    msgId,LASTUPDATEDDT,\n" +
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
				"where p9Data.Head.MessageType = 'MT5101'");

//		Table mt9999common_table = tEnv.sqlQuery("select * from mt9999common");
//		tEnv.toAppendStream(mt9999common_table, Row.class).print();
//		env.execute();
		
		// TODO 展开9999的5101回执01提单,已处理IMO、航次、总提单、分提单
		tEnv.executeSql("" +
				"create view mt9999bill as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    MessageID,\n" +
				"    FunctionCode,\n" +
				"    MessageType,\n" +
				"    SendTime,\n" +
				"    if(BorderTransportMeans_ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(BorderTransportMeans_JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,--航次\n" +
				"    ResponseType_Code,\n" +
				"    ResponseType_Text,--整个报文的回执类型\n" +
				"    if(TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as TransportContractDocument_ID, --总提单\n" +
				"    if(AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') as AssociatedTransportDocument_ID, --分提单\n" +
				"    ResponseType.Code as bill_ResponseType_Code,--提单的回执类型\n" +
				"    if(ResponseType.Text <> '', ResponseType.Text, 'N/A') as bill_ResponseType_Text --提单的回执类型\n" +
				"from (select * from mt9999common where Consignment is not null ) as temp2\n" +
				"    cross join unnest(Consignment) AS Consignment(TransportContractDocument,AssociatedTransportDocument,ResponseType)");
		
		Table mt9999bill_table = tEnv.sqlQuery("select * from mt9999bill");
		tEnv.toAppendStream(mt9999bill_table, Row.class).print();
//		env.execute();
		
		// TODO 展开9999的5101回执01箱,已处理IMO、航次、箱号
		tEnv.executeSql("" +
				"create view mt9999ctnr as\n" +
				"select\n" +
				"    msgId,\n" +
				"    LASTUPDATEDDT,\n" +
				"    MessageID,\n" +
				"    FunctionCode,\n" +
				"    MessageType,\n" +
				"    SendTime,\n" +
				"    if(BorderTransportMeans_ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as BorderTransportMeans_ID,--IMO\n" +
				"    if(BorderTransportMeans_JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(BorderTransportMeans_JourneyID, '[\\t\\n\\r]', ''))), 'N/A') as BorderTransportMeans_JourneyID,--航次\n" +
				"    ResponseType_Code, ResponseType_Text,--整个报文的回执类型\n" +
				"    if(EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(EquipmentIdentification.ID, '[\\t\\n\\r]', ''))), 'N/A') as EquipmentIdentification_ID,--箱号\n" +
				"    ResponseType.Code as ctnr_ResponseType_Code,--箱的回执类型\n" +
				"    if(ResponseType.Text <> '', ResponseType.Text, 'N/A') as ctnr_ResponseType_Text --箱的回执类型\n" +
				"from (select * from mt9999common where TransportEquipment is not null) as temp3\n" +
				"    cross join unnest(TransportEquipment) AS TransportEquipment(EquipmentIdentification,ResponseType)");

//		Table mt9999ctnr_table = tEnv.sqlQuery("select * from mt9999ctnr");
//		tEnv.toAppendStream(mt9999ctnr_table, Row.class).print();
//		env.execute();
		
		// TODO 卸船（散货）、提单
		tEnv.executeSql("" +
				"create view ship_unload as\n" +
				"select temp4.*,\n" +
				"  if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
				"  if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
				"from\n" +
				"(\n" +
				"    select\n" +
				"        mt5101common.msgId,\n" +
				"        mt5101common.BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO\n" +
				"        mt5101common.BorderTransportMeans_Name as VSL_NAME, --船名\n" +
				"        mt5101common.BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"        mt9999bill.TransportContractDocument_ID as BL_NO, --提单号\n" +
				"        'N/A' as MASTER_BL_NO,--主提单号\n" +
				"        'I' as I_E_MARK, --进出口\n" +
				"        'D3.3s' as BIZ_STAGE_NO, --业务环节节点\n" +
				"        'I_vslUnloading_coarriBulk' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"        mt5101common.BorderTransportMeans_CompletedDateTime as BIZ_TIME, --业务发生时间\n" +
				"        '1' as BIZ_STATUS_CODE, --业务状态代码\n" +
				"        mt9999bill.bill_ResponseType_Text as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"        mt5101common.LASTUPDATEDDT, --最后处理时间\n" +
				"        0 as ISDELETED, --是否删除\n" +
				"        uuid() as UUID,\n" +
				"        1 as BIZ_STATUS_IFFECTIVE\n" +
				"    from mt5101common join mt9999bill\n" +
				"        on  mt5101common.Head_MessageID=mt9999bill.MessageID --通过MessageID、\n" +
				"        and mt5101common.BorderTransportMeans_ID = mt9999bill.BorderTransportMeans_ID--IMO、\n" +
				"        and mt5101common.BorderTransportMeans_JourneyID = mt9999bill.BorderTransportMeans_JourneyID--航次\n" +
				"    ) as temp4\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO匹配\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO匹配\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名匹配\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名匹配\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',temp4.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',temp4.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp4.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=discharge_status&TYPE_CODE=',temp4.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table ship_unload_table = tEnv.sqlQuery("select * from ship_unload");
		tEnv.toAppendStream(ship_unload_table, Row.class).print();
//		env.execute();
		
		// TODO 进口理货(箱)
		// todo 9999的5101回执箱,已关联dim表取值(前面已处理IMO、航次、箱号)
		tEnv.executeSql("" +
				"create view ImportTallyCtnr as\n" +
				"select\n" +
				"    mt9999ctnr.msgId,\n" +
				"    mt9999ctnr.BorderTransportMeans_ID as VSL_IMO_NO,--船舶IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as VSL_NAME,--船名\n" +
				"    mt9999ctnr.BorderTransportMeans_JourneyID as VOYAGE,--航次\n" +
				"    if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_IMONO,--标准IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_VSLNAME,--标准船名\n" +
				"    EquipmentIdentification_ID as CTNR_NO,--箱号，给理货（箱）使用\n" +
				"    'I' as I_E_MARK,--进出口\n" +
				"    'D3.3m' as BIZ_STAGE_NO,--业务环节节点\n" +
				"    'I_vslUnloading_mt5101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
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
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D3.3m&SUB_STAGE_CODE=I_vslUnloading_mt5101' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999ctnr.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999ctnr.ctnr_ResponseType_Code) = dim4.key and 'TYPE_NAME' = dim4.hashkey --业务状态\n");

//		Table ImportTallyCtnr_table = tEnv.sqlQuery("select * from ImportTallyCtnr");
//		tEnv.toAppendStream(ImportTallyCtnr_table, Row.class).print();
//		env.execute();
		
		// TODO 进口理货(提单)(集装箱货)  ImportTallyCtnr与箱单关系表关联
		tEnv.executeSql("" +
				"create view ImportTallyBill_fromCTNR as\n" +
				"select\n" +
				"    ImportTallyCtnr.msgId,\n" +
				"    ImportTallyCtnr.VSL_IMO_NO,\n" +
				"    ImportTallyCtnr.VSL_NAME,\n" +
				"    ImportTallyCtnr.VOYAGE,\n" +
				"    ImportTallyCtnr.ACCURATE_IMONO,\n" +
				"    ImportTallyCtnr.ACCURATE_VSLNAME,\n" +
				"    oBCd.BL_NO,\n" +
				"    oBCd.MASTER_BL_NO,\n" +
				"    ImportTallyCtnr.I_E_MARK,\n" +
				"    ImportTallyCtnr.BIZ_STAGE_NO,\n" +
				"    ImportTallyCtnr.BIZ_STAGE_CODE,\n" +
				"    ImportTallyCtnr.BIZ_STAGE_NAME,\n" +
				"    ImportTallyCtnr.BIZ_TIME,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_CODE,\n" +
				"    ImportTallyCtnr.BIZ_STATUS,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_DESC,\n" +
				"    ImportTallyCtnr.LASTUPDATEDDT,\n" +
				"    ImportTallyCtnr.ISDELETED,\n" +
				"    ImportTallyCtnr.UUID,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyCtnr\n" +
				"    left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF ImportTallyCtnr.LASTUPDATEDDT as oBCd\n" +
				"    on  ImportTallyCtnr.VSL_IMO_NO = oBCd.VSL_IMO_NO\n" +
				"    and ImportTallyCtnr.VOYAGE = oBCd.VOYAGE\n" +
				"    and ImportTallyCtnr.CTNR_NO = oBCd.CTNR_NO\n" +
				"where oBCd.BL_NO is not null and oBCd.MASTER_BL_NO = 'N/A'");

//		Table ImportTallyBill_fromCTNR_table = tEnv.sqlQuery("select * from ImportTallyBill_fromCTNR");
//		tEnv.toAppendStream(ImportTallyBill_fromCTNR_table, Row.class).print();
//		env.execute();
		
		// TODO 进口理货(提单)(散货)
		tEnv.executeSql("" +
				"create view ImportTallyBill as\n" +
				"select\n" +
				"    mt9999bill.msgId,\n" +
				"    BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as VSL_NAME, --船名\n" +
				"    BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"    if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_IMONO, --标准IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_VSLNAME, --标准船名\n" +
				"    TransportContractDocument_ID as BL_NO, --提单号\n" +
				"    'N/A' as MASTER_BL_NO, --总提单号\n" +
				"    'I' as I_E_MARK, --进出口\n" +
				"    'D3.3m' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'I_vslUnloading_mt5101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"    TO_TIMESTAMP(concat(substr(mt9999bill.SendTime,1,14), '.', substr(mt9999bill.SendTime,15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME,--业务发生时间\n" +
				"    mt9999bill.bill_ResponseType_Code as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,--业务状态\n" +
				"    if(mt9999bill.bill_ResponseType_Text <> '', mt9999bill.bill_ResponseType_Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    mt9999bill.LASTUPDATEDDT,--最后处理时间\n" +
				"    if(mt9999bill.FunctionCode = '3', 1, 0) as ISDELETED, --标记是否删除\n" +
				"    uuid() as UUID,\n" +
				"    if(mt9999bill.bill_ResponseType_Code='01' and mt9999bill.bill_ResponseType_Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from mt9999bill\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999bill.BorderTransportMeans_ID) = dim1.key and 'VSL_NAME_EN' = dim1.hashkey --取船名和标准船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999bill.BorderTransportMeans_ID) = dim2.key and 'IMO_NO' = dim2.hashkey --取标准IMO\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D3.3m&SUB_STAGE_CODE=I_vslUnloading_mt5101' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999bill.bill_ResponseType_Code) = dim4.key and 'TYPE_NAME' = dim4.hashkey --业务状态\n");

//		Table ImportTallyBill_table = tEnv.sqlQuery("select * from ImportTallyBill");
//		tEnv.toAppendStream(ImportTallyBill_table, Row.class).print();
//		env.execute();
		
		// TODO 进口分拨理货(提单)(散货)
		tEnv.executeSql("" +
				"create view ImportDispatchTallyBill as\n" +
				"select\n" +
				"    mt9999bill.msgId,\n" +
				"    BorderTransportMeans_ID as VSL_IMO_NO, --船舶IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as VSL_NAME, --船名\n" +
				"    BorderTransportMeans_JourneyID as VOYAGE, --航次\n" +
				"    if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_IMONO, --标准IMO\n" +
				"    if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_VSLNAME, --标准船名\n" +
				"    AssociatedTransportDocument_ID as BL_NO, --提单号\n" +
				"    TransportContractDocument_ID as MASTER_BL_NO, --总提单号\n" +
				"    'I' as I_E_MARK, --进出口\n" +
				"    'D3.3h' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'I_ctnrDeconsolidation_mt5101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"    TO_TIMESTAMP(concat(substr(mt9999bill.SendTime,1,14), '.', substr(mt9999bill.SendTime,15)), 'yyyyMMddHHmmss.SSS') as BIZ_TIME,--业务发生时间\n" +
				"    mt9999bill.bill_ResponseType_Code as BIZ_STATUS_CODE, --业务状态代码\n" +
				"    if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS,--业务状态\n" +
				"    if(mt9999bill.bill_ResponseType_Text <> '', mt9999bill.bill_ResponseType_Text, 'N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
				"    mt9999bill.LASTUPDATEDDT,--最后处理时间\n" +
				"    if(mt9999bill.FunctionCode = '3', 1, 0) as ISDELETED, --标记是否删除\n" +
				"    uuid() as UUID,\n" +
				"    if(mt9999bill.bill_ResponseType_Code='01' and mt9999bill.bill_ResponseType_Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE\n" +
				"from   mt9999bill\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999bill.BorderTransportMeans_ID) = dim1.key and 'VSL_NAME_EN' = dim1.hashkey --取船名和标准船名\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',mt9999bill.BorderTransportMeans_ID) = dim2.key and 'IMO_NO' = dim2.hashkey --取标准IMO\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D3.3h&SUB_STAGE_CODE=I_ctnrDeconsolidation_mt5101' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF mt9999bill.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=',mt9999bill.bill_ResponseType_Code) = dim4.key and 'TYPE_NAME' = dim4.hashkey --业务状态\n" +
				"where 'N/A' <> mt9999bill.AssociatedTransportDocument_ID\n");

//		Table ImportDispatchTallyBill_table = tEnv.sqlQuery("select * from ImportDispatchTallyBill");
//		tEnv.toAppendStream(ImportDispatchTallyBill_table, Row.class).print();
//		env.execute();
		
		// TODO 进口分拨理货(提单)(集装箱货) ImportTallyCtnr与箱单关系表关联
		tEnv.executeSql("" +
				"create view ImportDispatchTallyBill_fromCTNR as\n" +
				"select\n" +
				"    ImportTallyCtnr.msgId,\n" +
				"    ImportTallyCtnr.VSL_IMO_NO,\n" +
				"    ImportTallyCtnr.VSL_NAME,\n" +
				"    ImportTallyCtnr.VOYAGE,\n" +
				"    ImportTallyCtnr.ACCURATE_IMONO,\n" +
				"    ImportTallyCtnr.ACCURATE_VSLNAME,\n" +
				"    oBCd.BL_NO,\n" +
				"    oBCd.MASTER_BL_NO,\n" +
				"    ImportTallyCtnr.I_E_MARK,\n" +
				"    'D3.3h' as BIZ_STAGE_NO, --业务环节节点\n" +
				"    'I_ctnrDeconsolidation_mt5101' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
				"    if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
				"    ImportTallyCtnr.BIZ_TIME,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_CODE,\n" +
				"    ImportTallyCtnr.BIZ_STATUS,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_DESC,\n" +
				"    ImportTallyCtnr.LASTUPDATEDDT,\n" +
				"    ImportTallyCtnr.ISDELETED,\n" +
				"    ImportTallyCtnr.UUID,\n" +
				"    ImportTallyCtnr.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyCtnr\n" +
				"    left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF ImportTallyCtnr.LASTUPDATEDDT as oBCd\n" +
				"        on  ImportTallyCtnr.VSL_IMO_NO = oBCd.VSL_IMO_NO\n" +
				"        and ImportTallyCtnr.VOYAGE = oBCd.VOYAGE\n" +
				"        and ImportTallyCtnr.CTNR_NO = oBCd.CTNR_NO\n" +
				"    left join redis_dim FOR SYSTEM_TIME AS OF ImportTallyCtnr.LASTUPDATEDDT as dim3 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=D3.3h&SUB_STAGE_CODE=I_ctnrDeconsolidation_mt5101' = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey --业务环节节点名称\n" +
				"where oBCd.BL_NO is not null and oBCd.MASTER_BL_NO <> 'N/A'");
		
		Table ImportDispatchTallyBill_fromCTNR_table = tEnv.sqlQuery("select * from ImportDispatchTallyBill_fromCTNR");
		tEnv.toAppendStream(ImportDispatchTallyBill_fromCTNR_table, Row.class).print();
//		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO SINK
		// TODO 卸船(散货)ship_unload,oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  sm1.VSL_IMO_NO,sm1.VSL_NAME,sm1.VOYAGE,sm1.ACCURATE_IMONO,sm1.ACCURATE_VSLNAME,\n" +
				"  sm1.BL_NO,sm1.MASTER_BL_NO,sm1.I_E_MARK,sm1.BIZ_STAGE_NO,sm1.BIZ_STAGE_CODE,\n" +
				"  sm1.BIZ_STAGE_NAME,sm1.BIZ_TIME,sm1.BIZ_STATUS_CODE,sm1.BIZ_STATUS,\n" +
				"  sm1.BIZ_STATUS_DESC,sm1.LASTUPDATEDDT,sm1.ISDELETED,sm1.UUID,sm1.BIZ_STATUS_IFFECTIVE\n" +
				"from ship_unload as sm1 left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF sm1.LASTUPDATEDDT as obd1\n" +
				"on sm1.VSL_IMO_NO = obd1.VSL_IMO_NO\n" +
				"and sm1.VOYAGE = obd1.VOYAGE\n" +
				"and sm1.BL_NO = obd1.BL_NO\n" +
				"and sm1.BIZ_STAGE_NO = obd1.BIZ_STAGE_NO\n" +
				"where (obd1.BIZ_TIME is null or sm1.BIZ_TIME>obd1.BIZ_TIME)\n" +
				"and sm1.BIZ_TIME is not null");
		
		// TODO 卸船(散货)ship_unload,kafka
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_COARRIBULK' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select sm2.UUID,\n" +
				"  sm2.VSL_IMO_NO,sm2.VSL_NAME,sm2.VOYAGE,sm2.ACCURATE_IMONO,sm2.ACCURATE_VSLNAME,\n" +
				"  sm2.BL_NO,sm2.MASTER_BL_NO,sm2.I_E_MARK,sm2.BIZ_STAGE_NO,sm2.BIZ_STAGE_CODE,\n" +
				"  sm2.BIZ_STAGE_NAME,sm2.BIZ_TIME,sm2.BIZ_STATUS_CODE,sm2.BIZ_STATUS,\n" +
				"  sm2.BIZ_STATUS_DESC,sm2.LASTUPDATEDDT,sm2.ISDELETED,sm2.BIZ_STATUS_IFFECTIVE\n" +
				"from ship_unload as sm2 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF sm2.LASTUPDATEDDT as ospd1\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_COARRIBULK'=ospd1.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd1.TABLE_NAME\n" +
				"where ospd1.ISCURRENT=1 and sm2.BIZ_TIME is not null) as temp5");
		
		// TODO 进口理货(箱)ImportTallyCtnr,oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_ctnr(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  tc2.VSL_IMO_NO,tc2.VSL_NAME,tc2.VOYAGE,tc2.ACCURATE_IMONO,tc2.ACCURATE_VSLNAME,\n" +
				"  tc2.CTNR_NO,tc2.I_E_MARK,tc2.BIZ_STAGE_NO,tc2.BIZ_STAGE_CODE,tc2.BIZ_STAGE_NAME,\n" +
				"  tc2.BIZ_TIME,tc2.BIZ_STATUS_CODE,tc2.BIZ_STATUS,tc2.BIZ_STATUS_DESC,\n" +
				"  tc2.LASTUPDATEDDT,tc2.ISDELETED,tc2.UUID,tc2.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyCtnr as tc2 left join oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF tc2.LASTUPDATEDDT as ocd1\n" +
				"on tc2.VSL_IMO_NO = ocd1.VSL_IMO_NO\n" +
				"and tc2.VOYAGE = ocd1.VOYAGE\n" +
				"and tc2.CTNR_NO = ocd1.CTNR_NO\n" +
				"and tc2.BIZ_STAGE_NO = ocd1.BIZ_STAGE_NO\n" +
				"where (ocd1.BIZ_TIME is null or tc2.BIZ_TIME>ocd1.BIZ_TIME)\n" +
				"and tc2.BIZ_TIME is not null");
		
		// TODO 进口理货(箱)ImportTallyCtnr,kafka
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_ctnr(GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT5101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from (\n" +
				"select tc1.UUID,\n" +
				"  tc1.VSL_IMO_NO,tc1.VSL_NAME,tc1.VOYAGE,tc1.ACCURATE_IMONO,tc1.ACCURATE_VSLNAME,\n" +
				"  tc1.CTNR_NO,tc1.I_E_MARK,tc1.BIZ_STAGE_NO,tc1.BIZ_STAGE_CODE,tc1.BIZ_STAGE_NAME,\n" +
				"  tc1.BIZ_TIME,tc1.BIZ_STATUS_CODE,tc1.BIZ_STATUS,tc1.BIZ_STATUS_DESC,\n" +
				"  tc1.LASTUPDATEDDT,tc1.ISDELETED,tc1.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyCtnr as tc1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tc1.LASTUPDATEDDT as ospd2\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT5101'=ospd2.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd2.TABLE_NAME\n" +
				"where ospd2.ISCURRENT=1 and tc1.BIZ_TIME is not null) as temp6");
		
		// TODO 进口理货 提单(集装箱货),Oracle  ImportTallyCtnr与箱单关系表关联的结果
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  tbfc.VSL_IMO_NO,tbfc.VSL_NAME,tbfc.VOYAGE,tbfc.ACCURATE_IMONO,tbfc.ACCURATE_VSLNAME,\n" +
				"  tbfc.BL_NO,tbfc.MASTER_BL_NO,tbfc.I_E_MARK,tbfc.BIZ_STAGE_NO,tbfc.BIZ_STAGE_CODE,\n" +
				"  tbfc.BIZ_STAGE_NAME,tbfc.BIZ_TIME,tbfc.BIZ_STATUS_CODE,tbfc.BIZ_STATUS,\n" +
				"  tbfc.BIZ_STATUS_DESC,tbfc.LASTUPDATEDDT,tbfc.ISDELETED,tbfc.UUID,tbfc.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyBill_fromCTNR as tbfc left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF tbfc.LASTUPDATEDDT as obd2\n" +
				"on tbfc.VSL_IMO_NO = obd2.VSL_IMO_NO\n" +
				"and tbfc.VOYAGE = obd2.VOYAGE\n" +
				"and tbfc.BL_NO = obd2.BL_NO\n" +
				"and tbfc.BIZ_STAGE_NO = obd2.BIZ_STAGE_NO\n" +
				"where (obd2.BIZ_TIME is null or tbfc.BIZ_TIME>obd2.BIZ_TIME)\n" +
				"and tbfc.BIZ_TIME is not null");
		
		// TODO 进口理货 提单(集装箱货),kafka  ImportTallyCtnr与箱单关系表关联的结果
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT5101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select tbfc1.UUID,\n" +
				"  tbfc1.VSL_IMO_NO,tbfc1.VSL_NAME,tbfc1.VOYAGE,tbfc1.ACCURATE_IMONO,tbfc1.ACCURATE_VSLNAME,\n" +
				"  tbfc1.BL_NO,tbfc1.MASTER_BL_NO,tbfc1.I_E_MARK,tbfc1.BIZ_STAGE_NO,tbfc1.BIZ_STAGE_CODE,\n" +
				"  tbfc1.BIZ_STAGE_NAME,tbfc1.BIZ_TIME,tbfc1.BIZ_STATUS_CODE,tbfc1.BIZ_STATUS,\n" +
				"  tbfc1.BIZ_STATUS_DESC,tbfc1.LASTUPDATEDDT,tbfc1.ISDELETED,tbfc1.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyBill_fromCTNR as tbfc1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tbfc1.LASTUPDATEDDT as ospd3\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT5101'=ospd3.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd3.TABLE_NAME\n" +
				"where ospd3.ISCURRENT=1 and tbfc1.BIZ_TIME is not null) as temp7");
		
		// TODO 进口理货 提单(散货),oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  tb1.VSL_IMO_NO,tb1.VSL_NAME,tb1.VOYAGE,tb1.ACCURATE_IMONO,tb1.ACCURATE_VSLNAME,\n" +
				"  tb1.BL_NO,tb1.MASTER_BL_NO,tb1.I_E_MARK,tb1.BIZ_STAGE_NO,tb1.BIZ_STAGE_CODE,\n" +
				"  tb1.BIZ_STAGE_NAME,tb1.BIZ_TIME,tb1.BIZ_STATUS_CODE,tb1.BIZ_STATUS,\n" +
				"  tb1.BIZ_STATUS_DESC,tb1.LASTUPDATEDDT,tb1.ISDELETED,tb1.UUID,tb1.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyBill as tb1 left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF tb1.LASTUPDATEDDT as obd3\n" +
				"on tb1.VSL_IMO_NO = obd3.VSL_IMO_NO\n" +
				"and tb1.VOYAGE = obd3.VOYAGE\n" +
				"and tb1.BL_NO = obd3.BL_NO\n" +
				"and tb1.BIZ_STAGE_NO = obd3.BIZ_STAGE_NO\n" +
				"where (obd3.BIZ_TIME is null or tb1.BIZ_TIME>obd3.BIZ_TIME)\n" +
				"and tb1.BIZ_TIME is not null");
		
		// TODO 进口理货 提单(散货),kafka
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT5101' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select tb2.UUID,\n" +
				"  tb2.VSL_IMO_NO,tb2.VSL_NAME,tb2.VOYAGE,tb2.ACCURATE_IMONO,tb2.ACCURATE_VSLNAME,\n" +
				"  tb2.BL_NO,tb2.MASTER_BL_NO,tb2.I_E_MARK,tb2.BIZ_STAGE_NO,tb2.BIZ_STAGE_CODE,\n" +
				"  tb2.BIZ_STAGE_NAME,tb2.BIZ_TIME,tb2.BIZ_STATUS_CODE,tb2.BIZ_STATUS,\n" +
				"  tb2.BIZ_STATUS_DESC,tb2.LASTUPDATEDDT,tb2.ISDELETED,tb2.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportTallyBill as tb2 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tb2.LASTUPDATEDDT as ospd4\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT5101'=ospd4.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd4.TABLE_NAME\n" +
				"where ospd4.ISCURRENT=1 and tb2.BIZ_TIME is not null) as temp8");
		
		// TODO 进口分拨理货 提单(集装箱货),Oracle  ImportDispatchTallyBill_fromCTNR与箱单关系表关联的结果
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  tbfc.VSL_IMO_NO,tbfc.VSL_NAME,tbfc.VOYAGE,tbfc.ACCURATE_IMONO,tbfc.ACCURATE_VSLNAME,\n" +
				"  tbfc.BL_NO,tbfc.MASTER_BL_NO,tbfc.I_E_MARK,tbfc.BIZ_STAGE_NO,tbfc.BIZ_STAGE_CODE,\n" +
				"  tbfc.BIZ_STAGE_NAME,tbfc.BIZ_TIME,tbfc.BIZ_STATUS_CODE,tbfc.BIZ_STATUS,\n" +
				"  tbfc.BIZ_STATUS_DESC,tbfc.LASTUPDATEDDT,tbfc.ISDELETED,tbfc.UUID,tbfc.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportDispatchTallyBill_fromCTNR as tbfc left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF tbfc.LASTUPDATEDDT as obd2\n" +
				"on tbfc.VSL_IMO_NO = obd2.VSL_IMO_NO\n" +
				"and tbfc.VOYAGE = obd2.VOYAGE\n" +
				"and tbfc.BL_NO = obd2.BL_NO\n" +
				"and tbfc.BIZ_STAGE_NO = obd2.BIZ_STAGE_NO\n" +
				"where (obd2.BIZ_TIME is null or tbfc.BIZ_TIME>obd2.BIZ_TIME)\n" +
				"and tbfc.BIZ_TIME is not null");
		
		// TODO 进口分拨理货 提单(集装箱货),kafka  ImportDispatchTallyBill_fromCTNR与箱单关系表关联的结果
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT5101_DISPATCH' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select tbfc1.UUID,\n" +
				"  tbfc1.VSL_IMO_NO,tbfc1.VSL_NAME,tbfc1.VOYAGE,tbfc1.ACCURATE_IMONO,tbfc1.ACCURATE_VSLNAME,\n" +
				"  tbfc1.BL_NO,tbfc1.MASTER_BL_NO,tbfc1.I_E_MARK,tbfc1.BIZ_STAGE_NO,tbfc1.BIZ_STAGE_CODE,\n" +
				"  tbfc1.BIZ_STAGE_NAME,tbfc1.BIZ_TIME,tbfc1.BIZ_STATUS_CODE,tbfc1.BIZ_STATUS,\n" +
				"  tbfc1.BIZ_STATUS_DESC,tbfc1.LASTUPDATEDDT,tbfc1.ISDELETED,tbfc1.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportDispatchTallyBill_fromCTNR as tbfc1 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tbfc1.LASTUPDATEDDT as ospd3\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT5101_DISPATCH'=ospd3.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd3.TABLE_NAME\n" +
				"where ospd3.ISCURRENT=1 and tbfc1.BIZ_TIME is not null) as temp7");
		
		// TODO 进口分拨理货 提单(散货),oracle
		statementSet.addInsertSql("" +
				"insert into oracle_track_biz_status_bill\n" +
				"    (VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,\n" +
				"    BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,\n" +
				"    BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,\n" +
				"    BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE)\n" +
				"select\n" +
				"  tb1.VSL_IMO_NO,tb1.VSL_NAME,tb1.VOYAGE,tb1.ACCURATE_IMONO,tb1.ACCURATE_VSLNAME,\n" +
				"  tb1.BL_NO,tb1.MASTER_BL_NO,tb1.I_E_MARK,tb1.BIZ_STAGE_NO,tb1.BIZ_STAGE_CODE,\n" +
				"  tb1.BIZ_STAGE_NAME,tb1.BIZ_TIME,tb1.BIZ_STATUS_CODE,tb1.BIZ_STATUS,\n" +
				"  tb1.BIZ_STATUS_DESC,tb1.LASTUPDATEDDT,tb1.ISDELETED,tb1.UUID,tb1.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportDispatchTallyBill as tb1\n" +
				"    left join oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF tb1.LASTUPDATEDDT as obd3\n" +
				"        on tb1.VSL_IMO_NO = obd3.VSL_IMO_NO\n" +
				"        and tb1.VOYAGE = obd3.VOYAGE\n" +
				"        and tb1.BL_NO = obd3.BL_NO\n" +
				"        and tb1.BIZ_STAGE_NO = obd3.BIZ_STAGE_NO\n" +
				"where (obd3.BIZ_TIME is null or tb1.BIZ_TIME>obd3.BIZ_TIME)\n" +
				"and tb1.BIZ_TIME is not null");
		
		// TODO 进口分拨理货 提单(散货),kafka
		statementSet.addInsertSql("" +
				"insert into kafka_track_biz_status_bill (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"select\n" +
				"  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_MT5101_DISPATCH' as APP_NAME,\n" +
				"  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
				"from\n" +
				"(select tb2.UUID,\n" +
				"  tb2.VSL_IMO_NO,tb2.VSL_NAME,tb2.VOYAGE,tb2.ACCURATE_IMONO,tb2.ACCURATE_VSLNAME,\n" +
				"  tb2.BL_NO,tb2.MASTER_BL_NO,tb2.I_E_MARK,tb2.BIZ_STAGE_NO,tb2.BIZ_STAGE_CODE,\n" +
				"  tb2.BIZ_STAGE_NAME,tb2.BIZ_TIME,tb2.BIZ_STATUS_CODE,tb2.BIZ_STATUS,\n" +
				"  tb2.BIZ_STATUS_DESC,tb2.LASTUPDATEDDT,tb2.ISDELETED,tb2.BIZ_STATUS_IFFECTIVE\n" +
				"from ImportDispatchTallyBill as tb2 left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF tb2.LASTUPDATEDDT as ospd4\n" +
				"on 'DATA_FLINK_FULL_FLINK_TRACING_MT5101_DISPATCH'=ospd4.APP_NAME\n" +
				"and 'DM.TRACK_BIZ_STATUS_BILL'=ospd4.TABLE_NAME\n" +
				"where ospd4.ISCURRENT=1 and tb2.BIZ_TIME is not null) as temp8");
		
		statementSet.execute();
	}
}
