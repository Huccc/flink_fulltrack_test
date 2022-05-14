package com.huc.flinksql_costco;

import com.easipass.flink.table.function.udsf.JsonToRowInCOSTCO;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class costco {
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
                "  `proctime` AS PROCTIME()\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'data-xpq-msg-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                "  'properties.group.id' = 'data-xpq-msg-parse-result',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'latest-offset'\n" +
                ")");

//        Table kafka_source_data_table = tEnv.sqlQuery("select * from kafka_source_data");
//        tEnv.toAppendStream(kafka_source_data_table, Row.class).print();
//        env.execute();

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
//                "    UUID STRING,\n" +
                "    BIZ_STATUS_IFFECTIVE int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'spe_data_sink',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                "  'properties.group.id' = 'spe_data_sink',\n" +
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
//                "    UUID STRING,\n" +
                "    BIZ_STATUS_IFFECTIVE int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'spe_data_sink',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                "  'properties.group.id' = 'spe_data_sink',\n" +
                "  'format' = 'json'\n" +
                ")");

        // TODO Oracle数据源
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
                "    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
                "    UUID STRING,\n" +
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

//        Table oracle_track_biz_status_bill_table = tEnv.sqlQuery("select * from oracle_track_biz_status_bill");
//        tEnv.toAppendStream(oracle_track_biz_status_bill_table, Row.class).print();
//        env.execute();

        // TODO Oracle箱状态信息
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

        // TODO 维表Redis
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

        // TODO 注册解析函数
        tEnv.createTemporarySystemFunction("JSON_TO_ROW_IN_COSTCO", JsonToRowInCOSTCO.class);

        // TODO 筛选COSTCO报文并解析:udf 模式
        tEnv.executeSql("" +
                "create view costcoTB as\n" +
                "select  msgId,\n" +
                "        JSON_TO_ROW_IN_COSTCO('{\"Msg\":' || parseData || '}') as pData,\n" +
                "        `proctime`\n" +
                "from kafka_source_data\n" +
                "WHERE msgType = 'message_data' AND bizId = 'COSTCO'");

//        Table costcoTB_table = tEnv.sqlQuery("select * from costcoTB");
//        tEnv.toAppendStream(costcoTB_table, Row.class).print();
//        env.execute();

        // TODO 展开获取COSTCO Ctn数据
        tEnv.executeSql("" +
                "create view costcoCtn as\n" +
                "select\n" +
                "    msgId AS MSGID,\n" +
                "    `proctime`,\n" +
                "    if(Ctn.DateOfPacking <> '',\n" +
                "        TO_TIMESTAMP(substr(Ctn.DateOfPacking,0,12) || '00.0', 'yyyyMMddHHmmss.S'),\n" +
                "        TO_TIMESTAMP(substr(HeadRecord.FileCreateTime,0,12) || '00.0', 'yyyyMMddHHmmss.S')\n" +
                "    ) AS BIZ_TIME,\n" +
                "    if(HeadRecord.VesselVoyageInformation.VesselImoNo <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(HeadRecord.VesselVoyageInformation.VesselImoNo, '[\\t\\n\\r]', ''))),'UN', ''), 'N/A') as VesselImoNo,--IMO\n" +
                "    if(HeadRecord.VesselVoyageInformation.VesselName <> '', UPPER(TRIM(REGEXP_REPLACE(HeadRecord.VesselVoyageInformation.VesselName, '[\\t\\n\\r]', ''))), 'N/A') as VesselName,--船名\n" +
                "    if(HeadRecord.VesselVoyageInformation.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(HeadRecord.VesselVoyageInformation.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as Voyage,--航次\n" +
                "    if(Ctn.ContainerNo <> '', UPPER(TRIM(REGEXP_REPLACE(Ctn.ContainerNo, '[\\t\\n\\r]', ''))), 'N/A') as ContainerNo,--箱号\n" +
                "    Ctn.BillOfLadingInformation as BillOfLadingInformation\n" +
                "from costcoTB\n" +
                "    CROSS JOIN unnest(costcoTB.pData.Msg) AS Msg(HeadRecord, TailRecord)\n" +
                "    CROSS JOIN unnest(Msg.HeadRecord.ContainerInformation) AS Ctn(RecordId ,\n" +
                "                                                                ContainerNo ,\n" +
                "                                                                ContainerSizeType ,\n" +
                "                                                                ContainerStatus ,\n" +
                "                                                                DateOfPacking ,\n" +
                "                                                                SealNo ,\n" +
                "                                                                ContainerOperatorCode ,\n" +
                "                                                                ContainerOperator ,\n" +
                "                                                                ContainerTareWeight ,\n" +
                "                                                                TotalWeightOfGoods ,\n" +
                "                                                                TotalVolume ,\n" +
                "                                                                TotalNumberOfPackages ,\n" +
                "                                                                TotalWeight ,\n" +
                "                                                                EirNo ,\n" +
                "                                                                OverLengthFront ,\n" +
                "                                                                OverLengthBack ,\n" +
                "                                                                OverWidthRight ,\n" +
                "                                                                OverWidthLeft ,\n" +
                "                                                                OverHeight ,\n" +
                "                                                                ContainerStuffingParty ,\n" +
                "                                                                AddressOfTheContainerStuffingParty ,\n" +
                "                                                                LicensePlateNoOfTrailer ,\n" +
                "                                                                ModeOfTransport ,\n" +
                "                                                                PortOfLoadingDischarge,\n" +
                "                                                                DangerousCargoInformation ,\n" +
                "                                                                RefrigeratedContainerInformation ,\n" +
                "                                                                BillOfLadingInformation)");

//        Table costcoCtn_table = tEnv.sqlQuery("select * from costcoCtn");
//        tEnv.toAppendStream(costcoCtn_table, Row.class).print();
//        env.execute();

        // TODO 装箱(箱)
        tEnv.executeSql("" +
                "create view packing_costco_ctn as\n" +
                "select temp4.*,\n" +
                "  if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
                "  if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
                "  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
                "  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
                "from\n" +
                "(\n" +
                "    select\n" +
                "        costcoCtn.MSGID AS MSGID,\n" +
                "        costcoCtn.VesselImoNo as VSL_IMO_NO, --船舶IMO\n" +
                "        costcoCtn.VesselName as VSL_NAME, --船名\n" +
                "        costcoCtn.Voyage as VOYAGE, --航次\n" +
                "        costcoCtn.ContainerNo as CTNR_NO, --箱号\n" +
                "        'E' as I_E_MARK, --进出口\n" +
                "        'C5.2' as BIZ_STAGE_NO, --业务环节节点\n" +
                "        'E_packing_costco' as BIZ_STAGE_CODE, --业务环节节点代码\n" +
                "        costcoCtn.BIZ_TIME as BIZ_TIME, --业务发生时间\n" +
                "        '1' as BIZ_STATUS_CODE, --业务状态代码\n" +
                "        'N/A' as BIZ_STATUS_DESC, --业务状态详细描述\n" +
                "        CAST(LOCALTIMESTAMP AS TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
                "        `proctime`,\n" +
                "        0 as ISDELETED, --是否删除\n" +
                "        BillOfLadingInformation,\n" +
                "        1 as BIZ_STATUS_IFFECTIVE, --状态有效标记\n" +
                "        uuid() as UUID" +
                "    from costcoCtn\n" +
                ") as temp4\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO匹配,找imo\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',temp4.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO匹配,找船名\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名匹配,找imo\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp4.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名匹配,找船名\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',temp4.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',temp4.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF temp4.`proctime` as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=load_status&TYPE_CODE=',temp4.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");

        Table packing_costco_ctn_table = tEnv.sqlQuery("select * from packing_costco_ctn");
        tEnv.toAppendStream(packing_costco_ctn_table, Row.class).print();
//        env.execute();

        // TODO 装箱(提单)
        tEnv.executeSql("" +
                "create view packing_costco_bill as\n" +
                "select\n" +
                "    MSGID,\n" +
                "    VSL_IMO_NO, --船舶IMO\n" +
                "    VSL_NAME, --船名\n" +
                "    VOYAGE, --航次\n" +
                "    if(Bill.MasterBLNo <> '', UPPER(TRIM(REGEXP_REPLACE(Bill.MasterBLNo, '[\\t\\n\\r]', ''))), 'N/A') as BL_NO, --提单号\n" +
                "    'N/A' as MASTER_BL_NO,--主提单号\n" +
                "    I_E_MARK, --进出口\n" +
                "    BIZ_STAGE_NO, --业务环节节点\n" +
                "    BIZ_STAGE_CODE, --业务环节节点代码\n" +
                "    BIZ_TIME, --业务发生时间\n" +
                "    BIZ_STATUS_CODE, --业务状态代码\n" +
                "    BIZ_STATUS_DESC, --业务状态详细描述\n" +
                "    LASTUPDATEDDT,\n" +
                "    `proctime`,\n" +
                "    ISDELETED, --是否删除\n" +
                "    ACCURATE_IMONO, --标准IMO\n" +
                "    ACCURATE_VSLNAME, --标准船名\n" +
                "    BIZ_STAGE_NAME, --业务环节节点名称\n" +
                "    BIZ_STATUS, --业务状态\n" +
                "    BIZ_STATUS_IFFECTIVE, --状态有效标记\n" +
                "    UUID " +
                "from packing_costco_ctn\n" +
                "    cross join unnest(packing_costco_ctn.BillOfLadingInformation) AS Bill(RecordId ,\n" +
                "                                                                        MasterBLNo ,\n" +
                "                                                                        PlaceCodeOfDelivery ,\n" +
                "                                                                        PlaceOfDelivery ,\n" +
                "                                                                        CargoInformation)");

        Table packing_costco_bill_table = tEnv.sqlQuery("select * from packing_costco_bill");
        tEnv.toAppendStream(packing_costco_bill_table, Row.class).print();
//        env.execute();

        StatementSet statementSet = tEnv.createStatementSet();

        // TODO SINK
        // todo 装箱(提单),oracle
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
                "                                          ISDELETED,\n" +
                "                                          BIZ_STATUS_IFFECTIVE,\n" +
                "                                          UUID)\n" +
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
                "          M.ISDELETED,\n" +
                "          M.BIZ_STATUS_IFFECTIVE,\n" +
                "          M.UUID" +
                "     FROM packing_costco_bill AS M\n" +
                "    LEFT JOIN oracle_track_biz_status_bill FOR SYSTEM_TIME AS OF M.`proctime` AS T\n" +
                "        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
                "        AND M.VOYAGE = T.VOYAGE\n" +
                "        AND M.BL_NO = T.BL_NO\n" +
                "        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
                "WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
                "AND M.BIZ_TIME IS NOT NULL");

        // TODO 装箱(箱),oracle
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
                "          M.BIZ_STATUS_IFFECTIVE" +
                "     FROM packing_costco_ctn AS M\n" +
                "    LEFT JOIN oracle_track_biz_status_ctnr FOR SYSTEM_TIME AS OF M.`proctime` AS T\n" +
                "        ON M.VSL_IMO_NO = T.VSL_IMO_NO\n" +
                "        AND M.VOYAGE = T.VOYAGE\n" +
                "        AND M.CTNR_NO = T.CTNR_NO\n" +
                "        AND M.BIZ_STAGE_NO = T.BIZ_STAGE_NO\n" +
                "WHERE (T.BIZ_TIME IS NULL OR M.BIZ_TIME>T.BIZ_TIME)\n" +
                "AND M.BIZ_TIME IS NOT NULL");

        // TODO 装箱(提单),kafka
        tEnv.executeSql("" +
                "INSERT INTO kafka_track_biz_status_bill (GID,\n" +
                "                                         APP_NAME,\n" +
                "                                         TABLE_NAME,\n" +
                "                                         SUBSCRIBE_TYPE,\n" +
                "                                         DATA)\n" +
                "   SELECT UUID AS GID,\n" +
                "          'DATA_FLINK_FULL_FLINK_TRACING_COSTCO' AS APP_NAME,\n" +
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
                "               ISDELETED,\n" +
                "               BIZ_STATUS_IFFECTIVE)\n" +
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
                "                  M.ISDELETED,\n" +
                "                  M.BIZ_STATUS_IFFECTIVE\n" +
                "             FROM packing_costco_bill AS M\n" +
                "            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.`proctime` AS P\n" +
                "                  ON 'DATA_FLINK_FULL_FLINK_TRACING_COSTCO'=P.APP_NAME\n" +
                "                  AND 'DM.TRACK_BIZ_STATUS_BILL'=P.TABLE_NAME\n" +
                "WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");

        // TODO 装箱(箱),kafka
        tEnv.executeSql("" +
                "INSERT INTO kafka_track_biz_status_ctnr (GID,\n" +
                "                                         APP_NAME,\n" +
                "                                         TABLE_NAME,\n" +
                "                                         SUBSCRIBE_TYPE,\n" +
                "                                         DATA)\n" +
                "   SELECT UUID AS GID,\n" +
                "          'DATA_FLINK_FULL_FLINK_TRACING_COSTCO' AS APP_NAME,\n" +
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
                "             FROM packing_costco_ctn AS M\n" +
                "            LEFT JOIN oracle_subscribe_papam_dim FOR SYSTEM_TIME AS OF M.`proctime` AS P\n" +
                "                  ON 'DATA_FLINK_FULL_FLINK_TRACING_COSTCO'=P.APP_NAME\n" +
                "                  AND 'DM.TRACK_BIZ_STATUS_CTNR'=P.TABLE_NAME\n" +
                "WHERE P.ISCURRENT=1 AND M.BIZ_TIME IS NOT NULL)");

        statementSet.execute();
    }
}
