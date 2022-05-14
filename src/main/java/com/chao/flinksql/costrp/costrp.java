package com.chao.flinksql.costrp;

import com.easipass.flink.table.function.udsf.JsonArrayToRowInCOSTRP;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class costrp {
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
                "'connector' = 'kafka',\n" +
                "  'topic' = 'source_topic',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'source_topic',\n" +
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'format' = 'json'\n" +
                "  )");

//        Table kafka_source_data_table = tEnv.sqlQuery("select * from kafka_source_data");
//        tEnv.toAppendStream(kafka_source_data_table, Row.class).print();
//        env.execute();

//        --注册ScalarFunction解析函数
//        CREATE FUNCTION parseCOSTRP AS 'com.easipass.flink.table.function.udsf.JsonToRowInCOSTRP';
//        tEnv.executeSql("CREATE FUNCTION parseCOSTRP AS 'com.easipass.flink.table.function.udsf.JsonArrayToRowInCOSTRP'");

        tEnv.createTemporarySystemFunction("parseCOSTRP", JsonArrayToRowInCOSTRP.class);

        tEnv.executeSql("create view parseTB as\n" +
                "  select msgId,LASTUPDATEDDT,parseCOSTRP(concat('{\"message\":',parseData,'}')).message as pData\n" +
                "  from kafka_source_data\n" +
                "  where bizId='COSTRP' and msgType='message_data'");

//        Table parseTB_table = tEnv.sqlQuery("select * from parseTB");
//        tEnv.toAppendStream(parseTB_table, Row.class).print();
//        env.execute();

        tEnv.executeSql("create view originalTB as\n" +
                "  select msgId,HeadRecord,Memo,TailRecord,LASTUPDATEDDT\n" +
                "  from (select * from parseTB where pData is not null) as tempTB1 cross join unnest(pData) AS pData(HeadRecord,Memo,TailRecord)");

//        Table originalTB_table = tEnv.sqlQuery("select * from originalTB");
//        tEnv.toAppendStream(originalTB_table, Row.class).print();
//        env.execute();

        tEnv.executeSql("" +
                "create view commonTB as\n" +
                "select msgId,VslVoyFields,ContainerInformation,FileFunction,Memo,TailRecord,LASTUPDATEDDT\n" +
                "from (select * from originalTB where HeadRecord is not null) as tempTB2 cross join unnest(HeadRecord) AS HeadRecord(RecordId,MessageType,FileDescription,FileFunction,EdiCodeOfSender,EdiCodeOfRecipient,FileCreateTime,CopTo,VslVoyFields,TotalOfContainer,ContainerInformation)");

//        Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
//        tEnv.toAppendStream(commonTB_table, Row.class).print();
//        env.execute();

        // TODO redis dim redis维度表
        tEnv.executeSql("" +
                "CREATE TABLE redis_dim (\n" +
                "  key String,\n" +
                "  hashkey String,\n" +
                "  res String\n" +
                ") WITH (\n" +
                "'connector.type' = 'redis',\n" +
                "  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
                "  'database.num' = '0',\n" +
                "  'operate.type' = 'hash',\n" +
                "  'redis.version' = '2.6'\n" +
                "  )");

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
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_BLCTNR',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                "  )");

        // TODO 获取公共字段
        tEnv.executeSql("" +
                "create view costrpCommon as\n" +
                "  select msgId,\n" +
                "         if(VslVoyFields.VesselImoNo <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(VslVoyFields.VesselImoNo, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO, --imo\n" +
                "         if(VslVoyFields.VesselName <> '', UPPER(TRIM(REGEXP_REPLACE(VslVoyFields.VesselName, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME, --船名\n" +
                "         if(VslVoyFields.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(VslVoyFields.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE, --航次\n" +
                "         VslVoyFields.ImportExportMark as I_E_MARK, --进出口\n" +
                "         'C6.1' as BIZ_STAGE_NO, --业务节点\n" +
                "         'E_portIn_costrp' as BIZ_STAGE_CODE, --业务节点代码\n" +
                "         '1' as BIZ_STATUS_CODE, --业务状态\n" +
                "         'N/A' as BIZ_STATUS_DESC, --业务状态描述\n" +
                "         LASTUPDATEDDT, --最后处理时间\n" +
                "         if(FileFunction = '6', 1, 0) as ISDELETED, --是否删除\n" +
                "         ContainerInformation --箱\n" +
                "  from commonTB");

//        Table costrpCommon_table = tEnv.sqlQuery("select * from costrpCommon");
//        tEnv.toAppendStream(costrpCommon_table, Row.class).print();
//        env.execute();

        // TODO 关联redis
        tEnv.executeSql("" +
                "create view costrpCommonWithDim as\n" +
                "select costrpCommon.*,\n" +
                "       if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
                "       if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
                "       if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务节点名称\n" +
                "       if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
                "from costrpCommon\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',costrpCommon.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',costrpCommon.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',costrpCommon.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名查\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',costrpCommon.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名查\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',costrpCommon.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',costrpCommon.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF costrpCommon.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=port_in_status&TYPE_CODE=',costrpCommon.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");

//        Table costrpCommonWithDim_table = tEnv.sqlQuery("select * from costrpCommonWithDim");
//        tEnv.toAppendStream(costrpCommonWithDim_table, Row.class).print();
//        env.execute();

        // TODO 结果箱表    展开箱，获取箱号
        tEnv.executeSql("" +
                "create view ctnrTB as\n" +
                "  select\n" +
                "  msgId,VSL_IMO_NO,VSL_NAME,VOYAGE,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,\n" +
                "  BIZ_STATUS_CODE,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,ACCURATE_IMONO,\n" +
                "  ACCURATE_VSLNAME,BIZ_STAGE_NAME,BIZ_STATUS,\n" +
                "  if(ContainerNo <> '', UPPER(TRIM(REGEXP_REPLACE(ContainerNo, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO, --箱号\n" +
                "  TO_TIMESTAMP(InPortInfo.TimeOfPort_In,'yyyyMMddHHmm') as BIZ_TIME, --业务发生时间\n" +
                "  uuid() as UUID, --UUID\n" +
                "  1 as BIZ_STATUS_IFFECTIVE --状态有效标记\n" +
                "  from (select * from costrpCommonWithDim where ContainerInformation is not null) as tempTB2\n" +
                "  cross join unnest(ContainerInformation) AS ContainerInformation(RecordId,ContainerNo,ContainerSize,ContainerStatus,DateOfPacking,ModeOfTransport,ContainerOperatorCode,ContainerOperator,ContainerTareWeight,TotalWeightOfGoods,TotalVolume,TotalNumberOfPackages,TotalWeight,EirNo,OverLengthFront,OverLengthBack,OverWidthRight,OverWidthLeft,OverHeight,ContainerStuffingParty,AddressOfTheContainerStuffingParty,LicensePlateNoOfTrailer,PortInMark,SealNo,LoadDischargePort,DangerousInformation,CargoInformation,InPortInfo,BlNo)");

        Table ctnrTB_table = tEnv.sqlQuery("select * from ctnrTB");
        tEnv.toAppendStream(ctnrTB_table, Row.class).print();
//        env.execute();

        // TODO 结果提单表   关联箱单关系表获取提单号
        tEnv.executeSql("" +
                "create view billTB as\n" +
                "  select ctnrTB.msgId,\n" +
                "         ctnrTB.VSL_IMO_NO,ctnrTB.VSL_NAME,ctnrTB.VOYAGE,ctnrTB.ACCURATE_IMONO,ctnrTB.ACCURATE_VSLNAME,\n" +
                "         obcd.BL_NO,obcd.MASTER_BL_NO,ctnrTB.I_E_MARK,ctnrTB.BIZ_STAGE_NO,ctnrTB.BIZ_STAGE_CODE,\n" +
                "         ctnrTB.BIZ_STAGE_NAME,ctnrTB.BIZ_TIME,ctnrTB.BIZ_STATUS_CODE,ctnrTB.BIZ_STATUS,\n" +
                "         ctnrTB.BIZ_STATUS_DESC,ctnrTB.LASTUPDATEDDT,ctnrTB.ISDELETED,ctnrTB.UUID,ctnrTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from ctnrTB left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF ctnrTB.LASTUPDATEDDT as obcd\n" +
                "  on ctnrTB.VSL_IMO_NO=obcd.VSL_IMO_NO\n" +
                "  and ctnrTB.VOYAGE=obcd.VOYAGE\n" +
                "  and ctnrTB.CTNR_NO=obcd.CTNR_NO\n" +
                "  where obcd.BL_NO is not null and obcd.ISDELETED=0");

        Table billTB_table = tEnv.sqlQuery("select * from billTB");
        tEnv.toAppendStream(billTB_table, Row.class).print();
//        env.execute();

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
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'ADM_BDPP.SUBSCRIBE_PARAM',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass',\n" +
                "  'lookup.cache.max-rows' = '100',\n" +
                "  'lookup.cache.ttl' = '100',\n" +
                "  'lookup.max-retries' = '3'\n" +
                "  )");

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
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                "  )");

        // TODO oracle箱表，维表
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
                "    UUID STRING,\n" +
                "    BIZ_STATUS_IFFECTIVE DECIMAL(22, 0),\n" +
                "    ISDELETED DECIMAL(22, 0),\n" +
                "    PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
                ") WITH (\n" +
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                "  )");

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
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                "  )");

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
                ")\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'sink_topic',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                "  'properties.group.id' = 'sink_topic',\n" +
                "  'format' = 'json'\n" +
                "  )");

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
                "'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "--'dialect-name' = 'Oracle',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                "  )");

        // TODO kafka sink表,箱
        tEnv.executeSql("" +
                "create table kafka_ctn(\n" +
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
                "      CTNR_NO STRING,\n" +
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
                ")\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'sink_topic',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.122:9092,192.168.129.123:9092,192.168.129.124:9092',\n" +
                "  'properties.group.id' = 'sink_topic',\n" +
                "  'format' = 'json'\n" +
                "  )");

        StatementSet statementSet = tEnv.createStatementSet();

        // TODO 结果箱表写入Oracle
        statementSet.addInsertSql("" +
                "insert into oracle_track_biz_status_ctnr\n" +
                "  select\n" +
                "  ctnrTB.VSL_IMO_NO,ctnrTB.VSL_NAME,ctnrTB.VOYAGE,ctnrTB.ACCURATE_IMONO,ctnrTB.ACCURATE_VSLNAME,\n" +
                "  ctnrTB.CTNR_NO,ctnrTB.I_E_MARK,ctnrTB.BIZ_STAGE_NO,ctnrTB.BIZ_STAGE_CODE,ctnrTB.BIZ_STAGE_NAME,\n" +
                "  ctnrTB.BIZ_TIME,ctnrTB.BIZ_STATUS_CODE,ctnrTB.BIZ_STATUS,ctnrTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,\n" +
                "  ctnrTB.ISDELETED,\n" +
                "  ctnrTB.UUID,\n" +
                "  ctnrTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from ctnrTB left join oracle_ctnr_dim FOR SYSTEM_TIME AS OF ctnrTB.LASTUPDATEDDT as ocd\n" +
                "  on ctnrTB.VSL_IMO_NO = ocd.VSL_IMO_NO\n" +
                "  and ctnrTB.VOYAGE = ocd.VOYAGE\n" +
                "  and ctnrTB.CTNR_NO = ocd.CTNR_NO\n" +
                "  and ctnrTB.BIZ_STAGE_NO = ocd.BIZ_STAGE_NO\n" +
                "  where (ocd.BIZ_TIME is null or ctnrTB.BIZ_TIME>ocd.BIZ_TIME)\n" +
                "  and ctnrTB.BIZ_TIME is not null");

        // TODO 结果提单表写入Oracle
        statementSet.addInsertSql("" +
                "insert into oracle_track_biz_status_bill\n" +
                "  select\n" +
                "  billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
                "  billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,\n" +
                "  billTB.BIZ_STAGE_NAME,billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,\n" +
                "  billTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billTB.ISDELETED,\n" +
                "  billTB.UUID,billTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from billTB left join oracle_bill_dim FOR SYSTEM_TIME AS OF billTB.LASTUPDATEDDT as obd\n" +
                "  on billTB.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
                "  and billTB.VOYAGE = obd.VOYAGE\n" +
                "  and billTB.BL_NO = obd.BL_NO\n" +
                "  and billTB.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
                "  where (obd.BIZ_TIME is null or billTB.BIZ_TIME>obd.BIZ_TIME)\n" +
                "  and billTB.BIZ_TIME is not null");

        // TODO 结果箱表写kafka
        statementSet.addInsertSql("" +
                "insert into kafka_ctn\n" +
                "  select\n" +
                "  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_COSTRP' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
                "  from\n" +
                "  (select\n" +
                "  ctnrTB.UUID,ctnrTB.VSL_IMO_NO,ctnrTB.VSL_NAME,ctnrTB.VOYAGE,ctnrTB.ACCURATE_IMONO,ctnrTB.ACCURATE_VSLNAME,\n" +
                "  ctnrTB.CTNR_NO,ctnrTB.I_E_MARK,ctnrTB.BIZ_STAGE_NO,ctnrTB.BIZ_STAGE_CODE,ctnrTB.BIZ_STAGE_NAME,\n" +
                "  ctnrTB.BIZ_TIME,ctnrTB.BIZ_STATUS_CODE,ctnrTB.BIZ_STATUS,ctnrTB.BIZ_STATUS_DESC,\n" +
                "  cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,ctnrTB.ISDELETED,ctnrTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from ctnrTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF ctnrTB.LASTUPDATEDDT as ospd1\n" +
                "  on 'DATA_FLINK_FULL_FLINK_TRACING_COSTRP'=ospd1.APP_NAME\n" +
                "  and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd1.TABLE_NAME\n" +
                "  where ospd1.ISCURRENT=1 and ctnrTB.BIZ_TIME is not null) as temp1");

        // TODO 结果提单表写kafka
        statementSet.addInsertSql("" +
                "insert into kafka_bill\n" +
                "  select\n" +
                "  UUID as GID,'DATA_FLINK_FULL_FLINK_TRACING_COSTRP' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,BIZ_STATUS_IFFECTIVE) as DATA\n" +
                "  from\n" +
                "  (select\n" +
                "  billTB.UUID,billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
                "  billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,billTB.BIZ_STAGE_NAME,\n" +
                "  billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,billTB.BIZ_STATUS_DESC,cast(LOCALTIMESTAMP as TIMESTAMP(3)) as LASTUPDATEDDT,billTB.ISDELETED,billTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from billTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF billTB.LASTUPDATEDDT as ospd2\n" +
                "  on 'DATA_FLINK_FULL_FLINK_TRACING_COSTRP'=ospd2.APP_NAME\n" +
                "  and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
                "  where ospd2.ISCURRENT=1 and billTB.BIZ_TIME is not null) as temp2");

        statementSet.execute();
    }
}
