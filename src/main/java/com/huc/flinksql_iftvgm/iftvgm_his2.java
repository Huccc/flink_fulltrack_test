package com.huc.flinksql_iftvgm;

import com.easipass.flink.table.function.udsf.JsonToRowInIFTVGM;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class iftvgm_his2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
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
                "  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR),\n" +
                "  wintime AS PROCTIME()\n" +
                ") WITH (\n" +
                "'connector' = 'kafka',\n" +
                "  'topic' = 'IFTVGM2',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'IFTVGM2',\n" +
                "  'scan.startup.mode' = 'group-offsets',\n" +
                "  'format' = 'json'\n" +
                "  )");

        // TODO Kafka配置表 维表
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
                "      UUID STRING,\n" +
                "      BIZ_STATUS_IFFECTIVE int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'IFTVGM_BILL_TEST',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'IFTVGM',\n" +
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
                "      UUID STRING,\n" +
                "      BIZ_STATUS_IFFECTIVE int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'IFTVGM_CTNR_TEST',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'IFTVGM',\n" +
                "  'format' = 'json'\n" +
                "  )");

        // TODO redis dim
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

        // TODO 注册IFTVGM解析函数
        tEnv.createTemporarySystemFunction("parseIFTVGM", JsonToRowInIFTVGM.class);

        // TODO 解析
        tEnv.executeSql("" +
                "create view sourceTB as\n" +
                "  select msgId, parseIFTVGM(concat('{\"message\":',parseData,'}')).message as pData, LASTUPDATEDDT,wintime,concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
                "  from kafka_source_data where bizId='IFTVGM' and msgType='message_data'");

//		Table sourceTB_table = tEnv.sqlQuery("select * from sourceTB");
//		tEnv.toAppendStream(sourceTB_table, Row.class).print();
//		env.execute();

        // TODO 获取船名航次箱信息
        tEnv.executeSql("" +
                "create view commonTB as\n" +
                "  select\n" +
                "  msgId,GID,wintime,LASTUPDATEDDT,\n" +
                "  if(VesselVoyageInformation.VslName <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.VslName, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
                "  if(VesselVoyageInformation.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
                "  ContainerDetail, --箱\n" +
                "  HeadRecord.FileCreateTime as FileCreateTime\n" +
                "from (select * from sourceTB where pData is not null) as tempTB2 cross join unnest(pData) AS pData(HeadRecord,VesselVoyageInformation,ContainerDetail,TrailerRecord)");

//		Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
//		tEnv.toAppendStream(commonTB_table, Row.class).print();
//		env.execute();

        // TODO 展开箱
        tEnv.executeSql("" +
                "create view withCTNR as\n" +
                "  select\n" +
                "  msgId,GID,wintime,VSL_NAME,VOYAGE,\n" +
                "  if(CtnrNo <> '', UPPER(TRIM(REGEXP_REPLACE(CtnrNo, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO,\n" +
                "--   TO_TIMESTAMP(VgmTime, 'yyyyMMddHHmm') as BIZ_TIME,\n" +
                "  if(VgmTime <> '',\n" +
                "     TO_TIMESTAMP(VgmTime, 'yyyyMMddHHmm'),\n" +
                "     TO_TIMESTAMP(FileCreateTime, 'yyyyMMddHHmm')) as BIZ_TIME,\n" +
                "  'N/A' as BIZ_STATUS_DESC,\n" +
                "  LASTUPDATEDDT\n" +
                "  from (select * from commonTB where ContainerDetail is not null) as tempTB1 cross join unnest(ContainerDetail) AS ContainerDetail(SealNo,WoodenPkgClass,VgmTime,PkgQtyInCtnr,CtnrSizeType,CargoNetWtInCtnr,Remark,VgmMethod,CtnrTareWt,RecId,CargoVolumeInCtnr,VgmLocation,CtnrNo,Vgm,Other,ShipperVgm)");
//		Table withCTNR_table = tEnv.sqlQuery("select * from withCTNR");
//		tEnv.toAppendStream(withCTNR_table, Row.class).print();
//		env.execute();

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

        // TODO 关联箱单关系表和redis获得IMO,ACCURATE_VSLNAME
        tEnv.executeSql("" +
                "create view withCTNR_DIM as\n" +
                "                select msgId,GID,wintime,\n" +
                "                if(obcd.VSL_IMO_NO <> '', obcd.VSL_IMO_NO, 'N/A') as VSL_IMO_NO,\n" +
                "                withCTNR.VSL_NAME,\n" +
                "                withCTNR.VOYAGE,\n" +
                "                if(obcd.VSL_IMO_NO <> '', obcd.VSL_IMO_NO, 'N/A') as ACCURATE_IMONO,\n" +
                "                if(obcd.ACCURATE_VSLNAME <> '', obcd.ACCURATE_VSLNAME, 'N/A') as ACCURATE_VSLNAME,\n" +
                "                withCTNR.CTNR_NO,\n" +
                "                'E' as I_E_MARK,\n" +
                "                'C8.1' as BIZ_STAGE_NO,\n" +
                "                'E_DockApply_vgm' as BIZ_STAGE_CODE,\n" +
                "                if(dim1.res <> '', dim1.res, 'N/A') as BIZ_STAGE_NAME,\n" +
                "                BIZ_TIME,\n" +
                "                'VG' as BIZ_STATUS_CODE,\n" +
                "                if(dim2.res <> '', dim2.res, 'N/A') as BIZ_STATUS,\n" +
                "                BIZ_STATUS_DESC,\n" +
                "                withCTNR.LASTUPDATEDDT,\n" +
                "                0 as ISDELETED,\n" +
                "                uuid() as UUID,\n" +
                "                1 as BIZ_STATUS_IFFECTIVE\n" +
                "                from withCTNR \n" +
                "                left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF withCTNR.LASTUPDATEDDT as obcd\n" +
                "                on withCTNR.VSL_NAME=obcd.VSL_NAME\n" +
                "                and withCTNR.VOYAGE=obcd.VOYAGE and obcd.ISDELETED=0\n" +
                "                left join redis_dim FOR SYSTEM_TIME AS OF withCTNR.LASTUPDATEDDT as dim1 on 'BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=C8.1&SUB_STAGE_CODE=E_DockApply_vgm' = dim1.key and 'SUB_STAGE_NAME' = dim1.hashkey\n" +
                "                left join redis_dim FOR SYSTEM_TIME AS OF withCTNR.LASTUPDATEDDT as dim2 on 'BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=e_iftvgm_status&TYPE_CODE=VG' = dim2.key and 'TYPE_NAME' = dim2.hashkey");

        Table withCTNR_DIM = tEnv.sqlQuery("select * from withCTNR_DIM");
        tEnv.toAppendStream(withCTNR_DIM, Row.class).print();
//        env.execute();

// TODO 获取业务主键的最大业务发生时间
        tEnv.executeSql("" +
                "create view withCTNR_tmp as\n" +
                "select\n" +
                "  VSL_IMO_NO,\n" +
                "  VOYAGE,\n" +
                "  CTNR_NO,\n" +
                "  BIZ_STAGE_NO,\n" +
                "  BIZ_STATUS_CODE,\n" +
                "  max(BIZ_TIME) as BIZ_TIME\n" +
                "FROM withCTNR_DIM\n" +
                "GROUP BY\n" +
                "  TUMBLE(wintime, INTERVAL '2' minute),\n" +
                "  VSL_IMO_NO,\n" +
                "  VOYAGE,\n" +
                "  CTNR_NO,\n" +
                "  BIZ_STAGE_NO,\n" +
                "  BIZ_STATUS_CODE");

        Table withCTNR_tmp = tEnv.sqlQuery("select * from withCTNR_tmp");
        tEnv.toAppendStream(withCTNR_tmp, Row.class).print();
//        env.execute();

        // TODO 获取业务主键最大业务发生时间的所有字段
        tEnv.executeSql("" +
                "create view withCTNR_RES as\n" +
                "select withCTNR_DIM.*\n" +
                "from  withCTNR_DIM join withCTNR_tmp\n" +
                "                           on withCTNR_DIM.VSL_IMO_NO=withCTNR_tmp.VSL_IMO_NO\n" +
                "                           and withCTNR_DIM.VOYAGE=withCTNR_tmp.VOYAGE\n" +
                "                           and withCTNR_DIM.CTNR_NO=withCTNR_tmp.CTNR_NO\n" +
                "                           and withCTNR_DIM.BIZ_STAGE_NO=withCTNR_tmp.BIZ_STAGE_NO\n" +
                "                           and withCTNR_DIM.BIZ_STATUS_CODE=withCTNR_tmp.BIZ_STATUS_CODE\n" +
                "                           and withCTNR_DIM.BIZ_TIME=withCTNR_tmp.BIZ_TIME");

        Table withCTNR_RES = tEnv.sqlQuery("select * from withCTNR_RES");
        tEnv.toAppendStream(withCTNR_RES, Row.class).print();
//        env.execute();


        // TODO 关联箱单关系表获得提单号
        tEnv.executeSql("" +
                "create view billTB as\n" +
                "  select msgId,GID,\n" +
                "         withCTNR_RES.VSL_IMO_NO,withCTNR_RES.VSL_NAME,withCTNR_RES.VOYAGE,withCTNR_RES.ACCURATE_IMONO,\n" +
                "         withCTNR_RES.ACCURATE_VSLNAME,obcd.BL_NO,obcd.MASTER_BL_NO,withCTNR_RES.I_E_MARK,\n" +
                "         withCTNR_RES.BIZ_STAGE_NO,withCTNR_RES.BIZ_STAGE_CODE,withCTNR_RES.BIZ_STAGE_NAME,\n" +
                "         withCTNR_RES.BIZ_TIME,withCTNR_RES.BIZ_STATUS_CODE,withCTNR_RES.BIZ_STATUS,\n" +
                "         withCTNR_RES.BIZ_STATUS_DESC,withCTNR_RES.LASTUPDATEDDT,withCTNR_RES.ISDELETED,\n" +
                "         withCTNR_RES.UUID,withCTNR_RES.BIZ_STATUS_IFFECTIVE\n" +
                "  from withCTNR_RES left join oracle_blctnr_dim FOR SYSTEM_TIME AS OF withCTNR_RES.LASTUPDATEDDT as obcd\n" +
                "  on withCTNR_RES.VSL_IMO_NO=obcd.VSL_IMO_NO\n" +
                "  and withCTNR_RES.VSL_NAME=obcd.VSL_NAME\n" +
                "  and withCTNR_RES.VOYAGE=obcd.VOYAGE\n" +
                "  and withCTNR_RES.CTNR_NO=obcd.CTNR_NO\n" +
                "  and obcd.ISDELETED=0");

        Table billTB_table = tEnv.sqlQuery("select * from billTB");
        tEnv.toAppendStream(billTB_table, Row.class).print();
//		env.execute();

        //
        StatementSet statementSet = tEnv.createStatementSet();

        // TODO 箱，写Oracle
        statementSet.addInsertSql("" +
                "insert into oracle_track_biz_status_ctnr\n" +
                "  select\n" +
                "  withCTNR_RES.VSL_IMO_NO,withCTNR_RES.VSL_NAME,withCTNR_RES.VOYAGE,\n" +
                "  withCTNR_RES.ACCURATE_IMONO,withCTNR_RES.ACCURATE_VSLNAME,\n" +
                "  withCTNR_RES.CTNR_NO,withCTNR_RES.I_E_MARK,withCTNR_RES.BIZ_STAGE_NO,\n" +
                "  withCTNR_RES.BIZ_STAGE_CODE,withCTNR_RES.BIZ_STAGE_NAME,\n" +
                "  withCTNR_RES.BIZ_TIME,withCTNR_RES.BIZ_STATUS_CODE,withCTNR_RES.BIZ_STATUS,\n" +
                "  withCTNR_RES.BIZ_STATUS_DESC,withCTNR_RES.LASTUPDATEDDT,withCTNR_RES.ISDELETED,\n" +
                "  withCTNR_RES.UUID,withCTNR_RES.BIZ_STATUS_IFFECTIVE\n" +
                "  from withCTNR_RES left join oracle_ctnr_dim FOR SYSTEM_TIME AS OF withCTNR_RES.LASTUPDATEDDT as ocd\n" +
                "  on withCTNR_RES.VSL_IMO_NO = ocd.VSL_IMO_NO\n" +
                "  and withCTNR_RES.VOYAGE = ocd.VOYAGE\n" +
                "  and withCTNR_RES.CTNR_NO = ocd.CTNR_NO\n" +
                "  and withCTNR_RES.BIZ_STAGE_NO = ocd.BIZ_STAGE_NO\n" +
                "  where (ocd.BIZ_TIME is null or withCTNR_RES.BIZ_TIME>ocd.BIZ_TIME)\n" +
                "  and withCTNR_RES.BIZ_TIME is not null");

        // TODO 箱，写kafka
        statementSet.addInsertSql("" +
                "insert into kafka_ctn\n" +
                "  select\n" +
                "  GID,'DATA_FLINK_FULL_FLINK_TRACING_VGM' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
                "  from\n" +
                "  (select\n" +
                "  withCTNR_RES.GID,withCTNR_RES.VSL_IMO_NO,withCTNR_RES.VSL_NAME,\n" +
                "  withCTNR_RES.VOYAGE,withCTNR_RES.ACCURATE_IMONO,withCTNR_RES.ACCURATE_VSLNAME,\n" +
                "  withCTNR_RES.CTNR_NO,withCTNR_RES.I_E_MARK,withCTNR_RES.BIZ_STAGE_NO,\n" +
                "  withCTNR_RES.BIZ_STAGE_CODE,withCTNR_RES.BIZ_STAGE_NAME,withCTNR_RES.BIZ_TIME,\n" +
                "  withCTNR_RES.BIZ_STATUS_CODE,withCTNR_RES.BIZ_STATUS,withCTNR_RES.BIZ_STATUS_DESC,\n" +
                "  withCTNR_RES.LASTUPDATEDDT,withCTNR_RES.ISDELETED,withCTNR_RES.UUID,withCTNR_RES.BIZ_STATUS_IFFECTIVE\n" +
                "  from withCTNR_RES left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF withCTNR_RES.LASTUPDATEDDT as ospd1\n" +
                "  on 'DATA_FLINK_FULL_FLINK_TRACING_VGM'=ospd1.APP_NAME\n" +
                "  and 'DM.TRACK_BIZ_STATUS_CTNR'=ospd1.TABLE_NAME\n" +
                "  where ospd1.ISCURRENT=1 and withCTNR_RES.BIZ_TIME is not null) as temp1");

        // TODO 提单，写Oracle
        statementSet.addInsertSql("" +
                "insert into oracle_track_biz_status_bill\n" +
                "  select\n" +
                "  billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
                "  billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,\n" +
                "  billTB.BIZ_STAGE_NAME,billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,\n" +
                "  billTB.BIZ_STATUS_DESC,billTB.LASTUPDATEDDT,billTB.ISDELETED,\n" +
                "  billTB.UUID,billTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from billTB left join oracle_bill_dim FOR SYSTEM_TIME AS OF billTB.LASTUPDATEDDT as obd\n" +
                "  on billTB.VSL_IMO_NO = obd.VSL_IMO_NO\n" +
                "  and billTB.VOYAGE = obd.VOYAGE\n" +
                "  and billTB.BL_NO = obd.BL_NO\n" +
                "  and billTB.BIZ_STAGE_NO = obd.BIZ_STAGE_NO\n" +
                "  where (obd.BIZ_TIME is null or billTB.BIZ_TIME>obd.BIZ_TIME)\n" +
                "  and billTB.BIZ_TIME is not null");

        // TODO 提单，写kafka
        statementSet.addInsertSql("" +
                "insert into kafka_bill\n" +
                "  select\n" +
                "  GID,'DATA_FLINK_FULL_FLINK_TRACING_VGM' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME, 'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,BL_NO,MASTER_BL_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED,UUID,BIZ_STATUS_IFFECTIVE) as DATA\n" +
                "  from\n" +
                "  (select\n" +
                "  billTB.GID,billTB.VSL_IMO_NO,billTB.VSL_NAME,billTB.VOYAGE,billTB.ACCURATE_IMONO,billTB.ACCURATE_VSLNAME,\n" +
                "  billTB.BL_NO,billTB.MASTER_BL_NO,billTB.I_E_MARK,billTB.BIZ_STAGE_NO,billTB.BIZ_STAGE_CODE,billTB.BIZ_STAGE_NAME,\n" +
                "  billTB.BIZ_TIME,billTB.BIZ_STATUS_CODE,billTB.BIZ_STATUS,billTB.BIZ_STATUS_DESC,billTB.LASTUPDATEDDT,billTB.ISDELETED,billTB.UUID,billTB.BIZ_STATUS_IFFECTIVE\n" +
                "  from billTB left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF billTB.LASTUPDATEDDT as ospd2\n" +
                "  on 'DATA_FLINK_FULL_FLINK_TRACING_VGM'=ospd2.APP_NAME\n" +
                "  and 'DM.TRACK_BIZ_STATUS_BILL'=ospd2.TABLE_NAME\n" +
                "  where ospd2.ISCURRENT=1 and billTB.BIZ_TIME is not null) as temp2");

        statementSet.execute();
    }
}
