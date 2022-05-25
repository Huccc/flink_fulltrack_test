package com.huc.flinksql_mt2101;

import com.easipass.flink.table.function.udsf.JsonToRowInMt2101;
import com.easipass.flink.table.function.udsf.JsonToRowInMt9999;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class mt2101 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE `KAFKA.DATA_XPQ_MSG_PARSE_RESULT` (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  `proctime` AS PROCTIME() + INTERVAL '8' HOURS\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'mt2101',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");
		
		tEnv.executeSql("CREATE TABLE `REDIS.DIM` (\n" +
				"  key STRING,\n" +
				"  field STRING,\n" +
				"  `value` STRING\n" +
				") WITH (\n" +
				"  'connector.type' = 'redis',\n" +
				"  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
				"  'operate.type' = 'hash',\n" +
				"  'database.num' = '0',\n" +
				"  'redis.version' = '2.6'\n" +
				")" +
				"");
		
		tEnv.executeSql("" +
				"CREATE TABLE `ORACLE.ADM_BDPP_SUBSCRIBE_PARAM` (\n" +
				"    APP_NAME STRING,\n" +
				"    TABLE_NAME STRING,\n" +
				"    SUBSCRIBE_TYPE STRING,\n" +
				"    SUBSCRIBER STRING,\n" +
				"    DB_URL STRING,\n" +
				"    KAFKA_SERVERS STRING,\n" +
				"    KAFKA_SINK_TOPIC STRING,\n" +
				"    ISCURRENT DECIMAL(10, 0), -- - 如果需要 LEFT JOIN 'TEMPRAL TABLE JOIN' && WHERE ISCURRENT=XXX，必须要 >= 10，否则会报 scala.MatchError: (CAST(CAST($7):DECIMAL(6, 0)):DECIMAL(10, 0),1:DECIMAL(10, 0))\n" +
				"    LASTUPDATEDDT TIMESTAMP(6),\n" +
				"    PRIMARY KEY(APP_NAME, TABLE_NAME) NOT ENFORCED\n" +
				") WITH (\n" +
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
				"    'username' = 'adm_bdpp',\n" +
				"    'password' = 'easipass',\n" +
				"    'lookup.cache.max-rows' = '100',\n" +
				"    'lookup.cache.ttl' = '600', -- 单位：s\n" +
				"    'lookup.max-retries' = '3'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE `ORACLE.TRACK_BIZ_STATUS_BILL_FOR_C71` (\n" +
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
				"    BIZ_TIME TIMESTAMP(3), -- SendTime\n" +
				"    BIZ_STATUS_CODE STRING,\n" +
				"    BIZ_STATUS STRING,\n" +
				"    BIZ_STATUS_DESC STRING,\n" +
				"    LASTUPDATEDDT TIMESTAMP(3),\n" +
				"    ISDELETED DECIMAL(1), -- Oracle NUMBER(1) <-> Flink DECIMAL(1)\n" +
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BL_NO, BIZ_STAGE_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉\n" +
				") WITH (\n" +
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
				"    'username' = 'dm',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE `ORACLE.TRACK_BIZ_STATUS_CTNR_FOR_C71` (\n" +
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
				"    ISDELETED DECIMAL(1), -- Oracle NUMBER(1) <-> Flink ORACLE DECIMAL(1)\n" +
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
				"    PRIMARY KEY(VSL_IMO_NO, VOYAGE, CTNR_NO, BIZ_STAGE_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 通过船舶编号、航次、提单匹配，不存在则插入，存在则更新\n" +
				") WITH (\n" +
				"    'connector' = 'jdbc',\n" +
				"    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"    'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
				"    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
				"    'username' = 'dm',\n" +
				"    'password' = 'easipass'\n" +
				")");
		
		// TODO 结果表
		tEnv.executeSql("" +
				"CREATE TABLE `KAFKA.TRACK_BIZ_STATUS_BILL_FOR_C71` (\n" +
				"  GID STRING,\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送\n" +
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
				"    ISDELETED INT,\n" +
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") WITH (\n" +
				" 'connector' = 'kafka',\n" +
				" 'topic' = 'topic-bdpevent-flink-push',\n" +
				" 'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				" 'format' = 'json'\n" +
				")");
		
		tEnv.executeSql("" +
				"CREATE TABLE `KAFKA.TRACK_BIZ_STATUS_CTNR_FOR_C71` (\n" +
				"  GID STRING,\n" +
				"  APP_NAME STRING,\n" +
				"  TABLE_NAME STRING,\n" +
				"  SUBSCRIBE_TYPE STRING, --  R: 补发，I: 实时推送\n" +
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
				"    ISDELETED INT, -- 对应 JSON number 类型\n" +
				"    UUID STRING,\n" +
				"    BIZ_STATUS_IFFECTIVE int\n" +
				"  )\n" +
				") WITH (\n" +
				" 'connector' = 'kafka',\n" +
				" 'topic' = 'topic-bdpevent-flink-push',\n" +
				" 'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				" 'format' = 'json'\n" +
				")");
		
		tEnv.createTemporarySystemFunction("UDF.JSON_TO_ROW_IN_MT2101", JsonToRowInMt2101.class);
		tEnv.createTemporarySystemFunction("UDF.JSON_TO_ROW_IN_MT9999", JsonToRowInMt9999.class);
		
		tEnv.executeSql("CREATE VIEW MT2101 AS\n" +
				"SELECT\n" +
				"  `UDF.JSON_TO_ROW_IN_MT2101`(parseData) AS Manifest,\n" +
				"  `proctime`, uuid() as UUID\n" +
				"FROM `KAFKA.DATA_XPQ_MSG_PARSE_RESULT`\n" +
				"WHERE msgType = 'message_data' AND bizId = 'MT2101'" +
				"");
		
//		Table MT2101 = tEnv.sqlQuery("select * from MT2101");
//		tEnv.toAppendStream(MT2101, Row.class).print();
		
		tEnv.executeSql("" +
				"CREATE VIEW MT9999 AS\n" +
				"SELECT *\n" +
				"FROM (\n" +
				"  SELECT\n" +
				"    `UDF.JSON_TO_ROW_IN_MT9999`(parseData) AS Manifest,\n" +
				"    `proctime`\n" +
				"  FROM `KAFKA.DATA_XPQ_MSG_PARSE_RESULT`\n" +
				"  WHERE msgType = 'message_data' AND bizId = 'MT9999'\n" +
				") -- MT9999 中过滤出 MT2101 的回执，尽早过滤以减少状态大小\n" +
				"WHERE Manifest.Head.MessageType = 'MT2101'");
		
//		Table MT9999 = tEnv.sqlQuery("select * from MT9999");
//		tEnv.toAppendStream(MT9999, Row.class).print();
		
		tEnv.executeSql("" +
				"CREATE VIEW BILL_CONSIGNMENT_IN_MT2101 AS\n" +
				"SELECT\n" +
				"  IF(M2.Manifest.Head.MessageID <> '', M2.Manifest.Head.MessageID, 'N/A') AS _MESSAGE_ID,\n" +
				"  IF(M2.Manifest.Declaration.BorderTransportMeans.ID <> '',\n" +
				"    REPLACE(UPPER(TRIM(REGEXP_REPLACE(M2.Manifest.Declaration.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') AS VSL_IMO_NO,\n" +
				"  IF(M2.Manifest.Declaration.BorderTransportMeans.JourneyID <> '',\n" +
				"    UPPER(TRIM(REGEXP_REPLACE(M2.Manifest.Declaration.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') AS VOYAGE,\n" +
				"  if(C2.AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(C2.AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))),  UPPER(TRIM(REGEXP_REPLACE(C2.TransportContractDocument.ID, '[\\t\\n\\r]', '')))) as BL_NO,\n" +
				"  if(C2.AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(C2.TransportContractDocument.ID, '[\\t\\n\\r]', ''))),  'N/A') AS MASTER_BL_NO,\n" +
				"  IF(C2.AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(C2.AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') AS Associated_ID,\n" +
				"  IF(C2.TransportContractDocument.ID <> '',\n" +
				"    UPPER(TRIM(REGEXP_REPLACE(C2.TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') AS Transport_ID,\n" +
				"  IF(M2.Manifest.Declaration.BorderTransportMeans.Name <> '',\n" +
				"    UPPER(TRIM(REGEXP_REPLACE(M2.Manifest.Declaration.BorderTransportMeans.Name, '[\\t\\n\\r]', ''))), 'N/A') AS VSL_NAME,\n" +
				"  IF(M2.FunctionCode = '3', 1, 0) AS ISDELETED, -- STRING TO INT\n" +
				"  C2.TransportEquipment AS _TRANSPORT_EQUIPMENT,  -- 货物（数组）,用于 “箱单关系表” 和 “箱状态表”\n" +
				"  IF(M2.ExtraInfo.sender <> '', M2.ExtraInfo.sender, 'N/A') AS _SENDER_ID,                 -- 用于 “箱单关系表”\n" +
				"  IF(M2.Head.FunctionCode <> '', M2.Head.FunctionCode, 'N/A') AS _FILE_FUNCTION,\n" +
				"  `proctime`,\n" +
				"  M2.UUID\n" +
				"FROM (select * from MT2101 where Declaration.Consignment is not null) AS M2\n" +
				"  CROSS JOIN UNNEST(M2.Manifest.Declaration.Consignment)\n" +
				"    AS C2(TransportContractDocument, AssociatedTransportDocument, GrossVolumeMeasure, ValueAmount, LoadingLocation,\n" +
				"        UnloadingLocation, GoodsReceiptPlace, TranshipmentLocation, TransitDestination, RoutingCountryCode,\n" +
				"        GoodsConsignedPlace, CustomsStatusCode, FreightPayment, ConsignmentPackaging, TotalGrossMassMeasure,\n" +
				"        PreviousCustomsDocument, DeliveryDestination, Handling, IntermediateCarrier, Consignee, Consignor,\n" +
				"        NotifyParty, UNDGContact, TransportEquipment, ConsignmentItem)");
		
//		Table BILL_CONSIGNMENT_IN_MT2101 = tEnv.sqlQuery("select * from BILL_CONSIGNMENT_IN_MT2101");
//		tEnv.toAppendStream(BILL_CONSIGNMENT_IN_MT2101, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW BILL_CONSIGNMENT_IN_MT9999 AS\n" +
				"SELECT\n" +
				"  IF(M9.Manifest.Head.MessageID <> '', M9.Manifest.Head.MessageID, 'N/A') AS _MESSAGE_ID,\n" +
				"  IF(M9.Manifest.Response.BorderTransportMeans.ID <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(M9.Manifest.Response.BorderTransportMeans.ID, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') AS VSL_IMO_NO,\n" +
				"  IF(M9.Manifest.Response.BorderTransportMeans.JourneyID <> '', UPPER(TRIM(REGEXP_REPLACE(M9.Manifest.Response.BorderTransportMeans.JourneyID, '[\\t\\n\\r]', ''))), 'N/A') AS VOYAGE,\n" +
				"  IF(C9.TransportContractDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(C9.TransportContractDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') AS Transport_ID,\n" +
				"  IF(C9.AssociatedTransportDocument.ID <> '', UPPER(TRIM(REGEXP_REPLACE(C9.AssociatedTransportDocument.ID, '[\\t\\n\\r]', ''))), 'N/A') AS Associated_ID,\n" +
				"  TO_TIMESTAMP(CONCAT_WS('.',SUBSTRING(M9.Head.SendTime FROM 1 FOR 14),SUBSTRING(M9.Head.SendTime FROM 15 FOR 17)), 'yyyyMMddHHmmss.SSS') AS BIZ_TIME, -- TO_TIMESTAMP() 存在 BUG，因此在 second 和 ms 之间增加一个 '.'\n" +
				"  IF(C9.ResponseType.Code <> '', C9.ResponseType.Code, 'N/A') AS BIZ_STATUS_CODE,\n" +
				"  IF(C9.ResponseType.Text <> '', C9.ResponseType.Text, 'N/A') AS BIZ_STATUS_DESC,\n" +
				"  `proctime`,\n" +
				"  IF(C9.ResponseType.Code ='01' and C9.ResponseType.Text not like '%删除%', 1, 0) as BIZ_STATUS_IFFECTIVE --有效状态标志，1为有效，0为无效 \n" +
				"FROM (select * from MT9999 where Response.Consignment is not null) AS M9\n" +
				"  CROSS JOIN UNNEST(M9.Response.Consignment) AS C9(TransportContractDocument, AssociatedTransportDocument, ResponseType)");
		
//		Table BILL_CONSIGNMENT_IN_MT9999 = tEnv.sqlQuery("select * from BILL_CONSIGNMENT_IN_MT9999");
//		tEnv.toAppendStream(BILL_CONSIGNMENT_IN_MT9999, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW BILL_WITHOUT_DIM AS\n" +
				"SELECT\n" +
				"  C2.VSL_IMO_NO AS VSL_IMO_NO,\n" +
				"  C2.VSL_NAME AS VSL_NAME,\n" +
				"  C2.VOYAGE AS VOYAGE,\n" +
				"  C2.BL_NO,\n" +
				"  C2.MASTER_BL_NO,\n" +
				"  'E' AS I_E_MARK,\n" +
				"  'C7.1' AS BIZ_STAGE_NO,\n" +
				"  'E_cusDecl_mt2101' AS BIZ_STAGE_CODE,\n" +
				"  C9.BIZ_TIME,\n" +
				"  C9.BIZ_STATUS_CODE,\n" +
				"  C9.BIZ_STATUS_DESC,\n" +
				"  CAST(C2.`proctime` AS TIMESTAMP(3)) AS LASTUPDATEDDT,\n" +
				"  C2.ISDELETED AS ISDELETED,\n" +
				"  C2._TRANSPORT_EQUIPMENT AS _TRANSPORT_EQUIPMENT, -- 货物（数组）\n" +
				"  C2._SENDER_ID AS _SENDER_ID,\n" +
				"  C2._FILE_FUNCTION AS _FILE_FUNCTION,\n" +
				"  C2._MESSAGE_ID AS _MESSAGE_ID,\n" +
				"  C2.`proctime`, -- C2.`proctime` = C9.`proctime`\n" +
				"  C2.UUID,\n" +
				"  C9.BIZ_STATUS_IFFECTIVE\n" +
				"FROM BILL_CONSIGNMENT_IN_MT2101 as C2\n" +
				"  JOIN BILL_CONSIGNMENT_IN_MT9999 AS C9\n" +
				"    ON C2._MESSAGE_ID = C9._MESSAGE_ID\n" +
				"      AND C2.VSL_IMO_NO = C9.VSL_IMO_NO\n" +
				"      AND C2.VOYAGE = C9.VOYAGE\n" +
				"      AND C2.Transport_ID = C9.Transport_ID\n" +
				"      AND C2.Associated_ID = C9.Associated_ID");
		
//		Table BILL_WITHOUT_DIM = tEnv.sqlQuery("select * from BILL_WITHOUT_DIM");
//		tEnv.toAppendStream(BILL_WITHOUT_DIM, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW BILL AS\n" +
				"SELECT\n" +
				"  VSL_IMO_NO,    -- PK\n" +
				"  VSL_NAME,\n" +
				"  VOYAGE,        -- PK\n" +
				"  IF(DIM_SHIP1.`value` <> '', DIM_SHIP1.`value`, 'N/A') AS ACCURATE_IMONO, -- 维表字段\n" +
				"  IF(DIM_SHIP2.`value` <> '', DIM_SHIP2.`value`, 'N/A') AS ACCURATE_VSLNAME, -- 维表字段\n" +
				"  BL_NO,         -- PK\n" +
				"  MASTER_BL_NO,  -- PK\n" +
				"  I_E_MARK,\n" +
				"  BIZ_STAGE_NO,  -- PK\n" +
				"  BIZ_STAGE_CODE,\n" +
				"  IF(DIM_BIZ_STAGE.`value` <> '', DIM_BIZ_STAGE.`value`, 'N/A') AS BIZ_STAGE_NAME, -- 维表字段\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  IF(DIM_COMMON_MINI.`value` <> '', DIM_COMMON_MINI.`value`, 'N/A') AS BIZ_STATUS, -- 维表字段\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  _TRANSPORT_EQUIPMENT, -- 货物（数组），用于 “箱状态表” 和 “箱单关系表”\n" +
				"  _SENDER_ID,           -- 用于 “箱单关系表”\n" +
				"  _FILE_FUNCTION,\n" +
				"  _MESSAGE_ID,\n" +
				"  `proctime`,\n" +
				"  concat(UUID,'_',BL_NO,'_',cast(BIZ_TIME as String)) as UUID,\n" +
				"  BIZ_STATUS_IFFECTIVE\n" +
				"FROM BILL_WITHOUT_DIM as B -- Redis 维表数据已满足 DIM_SHIP.ISCURRENT=1，另外使用 IMO_NO 去查询 IMO_NO 是想看看维表有没有空的，以维表的字段为主\n" +
				"  LEFT JOIN `REDIS.DIM` FOR SYSTEM_TIME AS OF B.`proctime` AS DIM_SHIP1\n" +
				"    ON DIM_SHIP1.key = CONCAT('BDCP:DIM:DIM_SHIP:IMO_NO=',REPLACE(B.VSL_IMO_NO,'UN','')) AND DIM_SHIP1.field = 'IMO_NO'\n" +
				"  LEFT JOIN `REDIS.DIM` FOR SYSTEM_TIME AS OF B.`proctime` AS DIM_SHIP2\n" +
				"    ON DIM_SHIP2.key = CONCAT('BDCP:DIM:DIM_SHIP:IMO_NO=',REPLACE(B.VSL_IMO_NO,'UN','')) AND DIM_SHIP2.field = 'VSL_NAME_EN'\n" +
				"  LEFT JOIN `REDIS.DIM` FOR SYSTEM_TIME AS OF B.`proctime` AS DIM_BIZ_STAGE\n" +
				"    ON DIM_BIZ_STAGE.key = CONCAT('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=', B.BIZ_STAGE_NO, '&SUB_STAGE_CODE=', B.BIZ_STAGE_CODE)\n" +
				"        AND DIM_BIZ_STAGE.field = 'SUB_STAGE_NAME'\n" +
				"  LEFT JOIN `REDIS.DIM` FOR SYSTEM_TIME AS OF B.`proctime` AS DIM_COMMON_MINI\n" +
				"    ON DIM_COMMON_MINI.key = CONCAT('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=mt9999_ack_type&TYPE_CODE=', B.BIZ_STATUS_CODE)\n" +
				"        AND DIM_COMMON_MINI.field = 'TYPE_NAME'");
		
//		Table BILL = tEnv.sqlQuery("select * from BILL");
//		tEnv.toAppendStream(BILL, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW BL_CTNR_WITHOUT_DIM AS\n" +
				"(\n" +
				"SELECT\n" +
				"  VSL_IMO_NO,    -- PK\n" +
				"  VSL_NAME,\n" +
				"  VOYAGE,\n" +
				"  ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,\n" +
				"  BL_NO,\n" +
				"  MASTER_BL_NO,\n" +
				"  if(T.EquipmentIdentification.ID <> '', UPPER(TRIM(REGEXP_REPLACE(T.EquipmentIdentification.ID,'[\\t\\n\\r]',''))), 'N/A') AS CTNR_NO, -- 逻辑上此处不可能为 'N/A'，箱号必须存在，但用户的操作有时候很迷惑\n" +
				"  I_E_MARK,\n" +
				"  BIZ_STAGE_NO,  -- PK\n" +
				"  BIZ_STAGE_CODE,\n" +
				"  BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  _SENDER_ID,\n" +
				"  _FILE_FUNCTION,\n" +
				"  _MESSAGE_ID,\n" +
				"  `proctime`,\n" +
				"  UUID,\n" +
				"  BIZ_STATUS_IFFECTIVE\n" +
				"FROM\n" +
				"  (SELECT * FROM BILL WHERE _TRANSPORT_EQUIPMENT IS NOT NULL) AS B\n" +
				"  CROSS JOIN UNNEST(B._TRANSPORT_EQUIPMENT)\n" +
				"    AS T(EquipmentIdentification,CharacteristicCode,SupplierPartyTypeCode,FullnessCode,SealID)\n" +
				")\n" +
				"UNION ALL\n" +
				"(\n" +
				"SELECT\n" +
				"  VSL_IMO_NO,\n" +
				"  VSL_NAME,\n" +
				"  VOYAGE,\n" +
				"  ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,\n" +
				"  BL_NO,\n" +
				"  MASTER_BL_NO,\n" +
				"  'N/A',          -- 直接设置为 ''，后续用于判断是否有箱\n" +
				"  I_E_MARK,\n" +
				"  BIZ_STAGE_NO,\n" +
				"  BIZ_STAGE_CODE,\n" +
				"  BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  _SENDER_ID,\n" +
				"  _FILE_FUNCTION,\n" +
				"  _MESSAGE_ID,\n" +
				"  `proctime`,\n" +
				"  UUID,\n" +
				"  BIZ_STATUS_IFFECTIVE\n" +
				"FROM\n" +
				"  BILL\n" +
				"WHERE _TRANSPORT_EQUIPMENT IS NULL -- “箱”（B._TRANSPORT_EQUIPMENT）可能为空，此时不能使用 CROSS JOIN 展开\n" +
				")");
		
		Table BL_CTNR_WITHOUT_DIM = tEnv.sqlQuery("select * from BL_CTNR_WITHOUT_DIM");
		tEnv.toAppendStream(BL_CTNR_WITHOUT_DIM, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"CREATE VIEW CTNR AS\n" +
				"SELECT\n" +
				"  VSL_IMO_NO,    -- PK\n" +
				"  VSL_NAME,\n" +
				"  VOYAGE,        -- PK\n" +
				"  ACCURATE_IMONO,\n" +
				"  ACCURATE_VSLNAME,\n" +
				"  CTNR_NO,       -- 箱号\n" +
				"  I_E_MARK,\n" +
				"  BIZ_STAGE_NO,  -- PK\n" +
				"  BIZ_STAGE_CODE,\n" +
				"  BIZ_STAGE_NAME,\n" +
				"  BIZ_TIME,\n" +
				"  BIZ_STATUS_CODE,\n" +
				"  BIZ_STATUS,\n" +
				"  BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  ISDELETED,\n" +
				"  _FILE_FUNCTION,\n" +
				"  `proctime`,\n" +
				"  concat(UUID, '_', CTNR_NO) as UUID,\n" +
				"  BIZ_STATUS_IFFECTIVE\n" +
				"FROM BL_CTNR_WITHOUT_DIM\n" +
				"WHERE CTNR_NO <> 'N/A'");
		
		Table CTNR = tEnv.sqlQuery("select * from CTNR");
		tEnv.toAppendStream(CTNR, Row.class).print();
//		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		statementSet.addInsertSql("" +
				"INSERT INTO `ORACLE.TRACK_BIZ_STATUS_BILL_FOR_C71` (VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, BL_NO, MASTER_BL_NO, I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED, UUID, BIZ_STATUS_IFFECTIVE)\n" +
				"SELECT\n" +
				"  B.VSL_IMO_NO, B.VSL_NAME, B.VOYAGE, B.ACCURATE_IMONO,\n" +
				"  B.ACCURATE_VSLNAME, B.BL_NO, B.MASTER_BL_NO, B.I_E_MARK,\n" +
				"  B.BIZ_STAGE_NO, B.BIZ_STAGE_CODE, B.BIZ_STAGE_NAME,\n" +
				"  B.BIZ_TIME, B.BIZ_STATUS_CODE, B.BIZ_STATUS, B.BIZ_STATUS_DESC,\n" +
				"  B.LASTUPDATEDDT, B.ISDELETED, B.UUID, B.BIZ_STATUS_IFFECTIVE\n" +
				"FROM (SELECT * FROM BILL WHERE _FILE_FUNCTION in( '9','0','5','3')) AS B\n" +
				"  LEFT JOIN `ORACLE.TRACK_BIZ_STATUS_BILL_FOR_C71` FOR SYSTEM_TIME AS OF B.`proctime` AS T\n" +
				"    ON T.VSL_IMO_NO = B.VSL_IMO_NO AND T.VOYAGE = B.VOYAGE AND T.BL_NO = B.BL_NO AND T.BIZ_STAGE_NO = B.BIZ_STAGE_NO and T.BIZ_STATUS_CODE = B.BIZ_STATUS_CODE\n" +
				"WHERE\n" +
				"  T.BIZ_TIME IS NULL /*insert*/ OR B.BIZ_TIME > T.BIZ_TIME");
		
		statementSet.addInsertSql("" +
				"INSERT INTO `KAFKA.TRACK_BIZ_STATUS_BILL_FOR_C71` (GID, APP_NAME, TABLE_NAME, SUBSCRIBE_TYPE, DATA)\n" +
				"SELECT\n" +
				"  UUID as GID,\n" +
				"  APP_NAME,\n" +
				"  TABLE_NAME,\n" +
				"  SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) AS DATA\n" +
				"FROM (\n" +
				"  SELECT\n" +
				"    P.APP_NAME AS APP_NAME,\n" +
				"    P.TABLE_NAME AS TABLE_NAME,\n" +
				"    P.SUBSCRIBE_TYPE AS SUBSCRIBE_TYPE,\n" +
				"    -- DATA --\n" +
				"    B.VSL_IMO_NO AS VSL_IMO_NO,\n" +
				"    B.VSL_NAME AS VSL_NAME,\n" +
				"    B.VOYAGE AS VOYAGE,\n" +
				"    B.ACCURATE_IMONO AS ACCURATE_IMONO,\n" +
				"    B.ACCURATE_VSLNAME AS ACCURATE_VSLNAME,\n" +
				"    B.BL_NO AS BL_NO,\n" +
				"    B.MASTER_BL_NO AS MASTER_BL_NO,\n" +
				"    B.I_E_MARK AS I_E_MARK,\n" +
				"    B.BIZ_STAGE_NO AS BIZ_STAGE_NO,\n" +
				"    B.BIZ_STAGE_CODE AS BIZ_STAGE_CODE,\n" +
				"    B.BIZ_STAGE_NAME AS BIZ_STAGE_NAME,\n" +
				"    B.BIZ_TIME AS BIZ_TIME,\n" +
				"    B.BIZ_STATUS_CODE AS BIZ_STATUS_CODE,\n" +
				"    B.BIZ_STATUS AS BIZ_STATUS,\n" +
				"    B.BIZ_STATUS_DESC AS BIZ_STATUS_DESC,\n" +
				"    B.LASTUPDATEDDT AS LASTUPDATEDDT,\n" +
				"    CAST(B.ISDELETED AS INT) AS ISDELETED,\n" +
				"    B.UUID,\n" +
				"    B.BIZ_STATUS_IFFECTIVE\n" +
				"  FROM BILL AS B\n" +
				"    LEFT JOIN `ORACLE.ADM_BDPP_SUBSCRIBE_PARAM` FOR SYSTEM_TIME AS OF B.`proctime` AS P\n" +
				"      ON P.APP_NAME = 'DATA_FLINK_FULL_FLINK_TRACING_MT2101' AND P.TABLE_NAME = 'DM.TRACK_BIZ_STATUS_BILL'\n" +
				"  WHERE P.ISCURRENT = 1 AND P.SUBSCRIBE_TYPE = 'I' -- 1表示允许输出到下游\n" +
				")");
		
		statementSet.addInsertSql("" +
				"INSERT INTO `ORACLE.TRACK_BIZ_STATUS_CTNR_FOR_C71` (VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, CTNR_NO, I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED, UUID, BIZ_STATUS_IFFECTIVE)\n" +
				"SELECT\n" +
				"    C.VSL_IMO_NO,\n" +
				"    C.VSL_NAME,\n" +
				"    C.VOYAGE,\n" +
				"    C.ACCURATE_IMONO,\n" +
				"    C.ACCURATE_VSLNAME,\n" +
				"    C.CTNR_NO,\n" +
				"    C.I_E_MARK,\n" +
				"    C.BIZ_STAGE_NO,\n" +
				"    C.BIZ_STAGE_CODE,\n" +
				"    C.BIZ_STAGE_NAME,\n" +
				"    C.BIZ_TIME,\n" +
				"    C.BIZ_STATUS_CODE,\n" +
				"    C.BIZ_STATUS,\n" +
				"    C.BIZ_STATUS_DESC,\n" +
				"    C.LASTUPDATEDDT,\n" +
				"    C.ISDELETED,\n" +
				"    C.UUID,\n" +
				"    C.BIZ_STATUS_IFFECTIVE\n" +
				"FROM (SELECT * FROM CTNR WHERE _FILE_FUNCTION = '9' OR _FILE_FUNCTION = '0' OR _FILE_FUNCTION = '5' OR _FILE_FUNCTION = '3') AS C\n" +
				"  LEFT JOIN `ORACLE.TRACK_BIZ_STATUS_CTNR_FOR_C71` FOR SYSTEM_TIME AS OF C.`proctime` AS T\n" +
				"    ON T.VSL_IMO_NO = C.VSL_IMO_NO AND T.VOYAGE = C.VOYAGE AND T.CTNR_NO = C.CTNR_NO AND T.BIZ_STAGE_NO = C.BIZ_STAGE_NO and T.BIZ_STATUS_CODE = C.BIZ_STATUS_CODE\n" +
				"WHERE\n" +
				"  T.BIZ_TIME IS NULL /*insert*/ OR C.BIZ_TIME > T.BIZ_TIME");
		
		statementSet.addInsertSql("" +
				"INSERT INTO `KAFKA.TRACK_BIZ_STATUS_CTNR_FOR_C71` (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
				"SELECT\n" +
				"  UUID as GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,\n" +
				"  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) AS DATA\n" +
				"FROM (\n" +
				"  SELECT\n" +
				"    P.APP_NAME AS APP_NAME,P.TABLE_NAME AS TABLE_NAME,P.SUBSCRIBE_TYPE AS SUBSCRIBE_TYPE,\n" +
				"    -- DATA --\n" +
				"    C.VSL_IMO_NO,C.VSL_NAME,C.VOYAGE,C.ACCURATE_IMONO,C.ACCURATE_VSLNAME,C.CTNR_NO,C.I_E_MARK,C.BIZ_STAGE_NO,C.BIZ_STAGE_CODE,C.BIZ_STAGE_NAME,C.BIZ_TIME,C.BIZ_STATUS_CODE,C.BIZ_STATUS,C.BIZ_STATUS_DESC,C.LASTUPDATEDDT AS LASTUPDATEDDT,CAST(C.ISDELETED AS INT) AS ISDELETED, UUID, BIZ_STATUS_IFFECTIVE\n" +
				"  FROM CTNR AS C\n" +
				"    LEFT JOIN `ORACLE.ADM_BDPP_SUBSCRIBE_PARAM` FOR SYSTEM_TIME AS OF C.`proctime` AS P\n" +
				"      ON P.APP_NAME = 'DATA_FLINK_FULL_FLINK_TRACING_MT2101' AND P.TABLE_NAME = 'DM.TRACK_BIZ_STATUS_CTNR'\n" +
				"      WHERE P.ISCURRENT = 1 AND P.SUBSCRIBE_TYPE = 'I' -- 1表示允许输出到下游\n" +
				")");
		
		statementSet.execute();
	}
}
