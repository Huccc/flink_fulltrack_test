package com.huc.flinksql_cuschk;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class letpas {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, bsSettings);
        env.setParallelism(1);
        tEnv.executeSql("" +
                "CREATE TABLE sourceTB (\n" +
                "  msgId STRING,\n" +
                "  bizId STRING,\n" +
                "  msgType STRING,\n" +
                "  bizUniqueId STRING,\n" +
                "  destination STRING,\n" +
                "  parseData STRING,\n" +
                "  LASTUPDATEDDT AS PROCTIME() + INTERVAL '8' HOUR\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-bdpcollect-db-parse-result',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'flink-sql-full-link-tracing-letpas',\n" +
                "  'format' = 'json',\n" +
                "  'scan.startup.mode' = 'group-offsets'\n" +
                ")");
        tEnv.executeSql("CREATE TABLE oracle_track_biz_status_bill (\n" +
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
                "    PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                ")\n");
        tEnv.executeSql("CREATE TABLE oracle_track_biz_status_ctnr (\n" +
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
                "    PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                ")\n");
        tEnv.executeSql("CREATE view kafka_edi_cnc201_vslbillinfo AS\n" +
                "SELECT\n" +
                "  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS vslbillinfo,\n" +
                "  msgId, LASTUPDATEDDT\n" +
                "FROM sourceTB\n" +
                "WHERE bizId = 'ogg_data'\n" +
                "  AND destination = 'SRC_XIB3.EDI_CNC201_VSLBILLINFO'\n");
        tEnv.executeSql("CREATE view kafka_edi_cnc201_ctninfo AS\n" +
                "SELECT\n" +
                "  STR_TO_MAP(regexp_replace(regexp_replace(regexp_replace(parseData, '\"', ''), '\\{', ''), '\\}', ''), ',', ':') AS ctninfo,\n" +
                "  msgId, LASTUPDATEDDT\n" +
                "FROM sourceTB\n" +
                "WHERE bizId = 'ogg_data' AND destination = 'SRC_XIB3.EDI_CNC201_CTNINFO'\n");
        tEnv.executeSql("create table kafka_sink(\n" +
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
                "    ISDELETED int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-bdpevent-flink-push',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
                "  'format' = 'json'\n" +
                ")\n");
        tEnv.executeSql("create table kafka_sink1(\n" +
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
                "    ISDELETED int\n" +
                "  )\n" +
                ") with (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'topic-bdpevent-flink-push',\n" +
                "  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
                "  'properties.group.id' = 'topic-bdpevent-flink-push',\n" +
                "  'format' = 'json'\n" +
                ")\n");
        tEnv.executeSql("CREATE TABLE redis_dim (\n" +
                "  key String,\n" +
                "  hashkey String,\n" +
                "  res String\n" +
                ") WITH (\n" +
                "  'connector.type' = 'redis',\n" +
                "  'redis.ip' = '192.168.129.121:6379,192.168.129.122:6379,192.168.129.123:6379,192.168.129.121:7379,192.168.129.122:7379,192.168.129.123:7379',\n" +
                "  'database.num' = '0',\n" +
                "  'operate.type' = 'hash',\n" +
                "  'redis.version' = '2.6'\n" +
                ")\n");
        tEnv.executeSql("CREATE TABLE oracle_subscribe_papam_dim (\n" +
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
                "  'lookup.cache.ttl' = '100',\n" +
                "  'lookup.max-retries' = '3'\n" +
                ")\n");
        tEnv.executeSql("CREATE TABLE oracle_bill_dim (\n" +
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
                "    PRIMARY KEY(VSL_IMO_NO,VOYAGE,BL_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                ")\n");
        tEnv.executeSql("CREATE TABLE oracle_ctnr_dim (\n" +
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
                "    PRIMARY KEY(VSL_IMO_NO,VOYAGE,CTNR_NO,BIZ_STAGE_NO) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "  'table-name' = 'DM.TRACK_BIZ_STATUS_CTNR',\n" +
                "  'driver' = 'oracle.jdbc.OracleDriver',\n" +
                "  'username' = 'xqwu',\n" +
                "  'password' = 'easipass'\n" +
                ")\n");
        tEnv.executeSql("create view resbill as\n" +
                "  select a.*,\n" +
                "    if(b.res <> '', b.res, if(b4.res <> '', b4.res, 'N/A')) as ACCURATE_IMONO, --标准IMO编号\n" +
                "    if(b1.res <> '', b1.res, if(b5.res <> '', b5.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
                "    if(b2.res <> '', b2.res, 'N/A') as BIZ_STAGE_NAME, --业务环节节点名称\n" +
                "    if(b3.res <> '', b3.res, 'N/A') as BIZ_STATUS --业务状态\n" +
                "from\n" +
                "  (select\n" +
                "    if(TRIM(vslbillinfo['IMONO']) <> '' and TRIM(vslbillinfo['IMONO']) <> 'null', REPLACE(UPPER(TRIM(REGEXP_REPLACE(vslbillinfo['IMONO'], '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO, --船舶IMO编号\n" +
                "    if(TRIM(vslbillinfo['VSLNAME']) <> '' and TRIM(vslbillinfo['VSLNAME']) <> 'null', UPPER(TRIM(REGEXP_REPLACE(vslbillinfo['VSLNAME'], '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME, --船名\n" +
                "    if(TRIM(vslbillinfo['VOYAGE']) <> '' and TRIM(vslbillinfo['VOYAGE']) <> 'null', UPPER(TRIM(REGEXP_REPLACE(vslbillinfo['VOYAGE'], '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE, --航次\n" +
                "    vslbillinfo['BLNO'] as BL_NO, --提运单号\n" +
                "    vslbillinfo['IEFLAG'] as I_E_MARK, --进出口标记\n" +
                "    if(CHAR_LENGTH(vslbillinfo['TIME_STAMP'])=17,\n" +
                "      TO_TIMESTAMP(concat(substr(vslbillinfo['TIME_STAMP'],1,14),'.',substr(vslbillinfo['TIME_STAMP'],15)), 'yyyyMMddHHmmss.SSS'),\n" +
                "      TO_TIMESTAMP(vslbillinfo['TIME_STAMP'], 'yyyyMMddHHmmss')) as BIZ_TIME, --业务发生时间\n" +
                "    if(CHAR_LENGTH(vslbillinfo['ENTRYID'])=16 or substr(vslbillinfo['ENTRYID'],1,2)='00','转关','N/A') as BIZ_STATUS_DESC, --业务状态详细描述\n" +
                "    'N/A' as MASTER_BL_NO, --总提运单号\n" +
                "    case vslbillinfo['IEFLAG'] when 'E' then 'C7.9' when 'I' then 'D6.11' else 'N/A' END as BIZ_STAGE_NO,--业务环节节点\n" +
                "    case vslbillinfo['IEFLAG'] when 'E' then 'E_cusDecl_letpas' when 'I' then 'I_cusDecl_cnc112' else 'N/A' END as BIZ_STAGE_CODE, --业务环节节点代码\n" +
                "    '1' as BIZ_STATUS_CODE, --业务状态代码\n" +
                "    LASTUPDATEDDT, --目标最后更新时间\n" +
                "    0 as ISDELETED, --标记是否删除\n" +
                "    msgId as BillmsgId, --用来在kafka唯一标识记录，这是提单表的\n" +
                "    vslbillinfo['MSGLOGID'] as MSGLOGID --用来与箱join\n" +
                "  from kafka_edi_cnc201_vslbillinfo ) as a\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',a.VSL_IMO_NO)=b.key and 'IMO_NO'=b.hashkey --通过imo匹配\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',a.VSL_IMO_NO)=b1.key and 'VSL_NAME_EN'=b1.hashkey --通过imo匹配\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b4 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',a.VSL_NAME) = b4.key and 'IMO_NO' = b4.hashkey --通过船名匹配\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',a.VSL_NAME) = b5.key and 'VSL_NAME_EN' = b5.hashkey --通过船名匹配\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b2 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',a.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',a.BIZ_STAGE_CODE)=b2.key and 'SUB_STAGE_NAME'=b2.hashkey\n" +
                "left join redis_dim FOR SYSTEM_TIME AS OF a.LASTUPDATEDDT as b3 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=cus_letpas_status&TYPE_CODE=',a.BIZ_STATUS_CODE)=b3.key and 'TYPE_NAME'=b3.hashkey\n" +
                "\n");

        tEnv.executeSql("create view rightTB as\n" +
                "select\n" +
                "  ctninfo['PARENTLOGID'] as PARENTLOGID,ctninfo['CTNNO'] as CTNR_NO,msgId,LASTUPDATEDDT\n" +
                "from kafka_edi_cnc201_ctninfo\n" +
                "\n");
        tEnv.executeSql("--关联获取箱表\n" +
                "create view resctn as\n" +
                "select\n" +
                "  rightTB.msgId as CtnrmsgId,\n" +
                "  resbill.VSL_IMO_NO,resbill.VSL_NAME,resbill.VOYAGE,resbill.ACCURATE_IMONO,resbill.ACCURATE_VSLNAME,\n" +
                "  rightTB.CTNR_NO,resbill.I_E_MARK,resbill.BIZ_STAGE_NO,resbill.BIZ_STAGE_CODE,resbill.BIZ_STAGE_NAME,\n" +
                "  resbill.BIZ_TIME,resbill.BIZ_STATUS_CODE,resbill.BIZ_STATUS,resbill.BIZ_STATUS_DESC,\n" +
                "  rightTB.LASTUPDATEDDT,resbill.ISDELETED\n" +
                "from resbill join rightTB\n" +
                "  on resbill.MSGLOGID=rightTB.PARENTLOGID\n");

        StatementSet statementSet = tEnv.createStatementSet();
        statementSet.addInsertSql("insert into kafka_sink (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
                "select\n" +
                "  BillmsgId as GID,\n" +
                "  'DATA_FLINK_FULL_FLINK_TRACING_LETPAS' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_BILL' as TABLE_NAME,\n" +
                "  'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO, VSL_NAME, VOYAGE, ACCURATE_IMONO, ACCURATE_VSLNAME, BL_NO, MASTER_BL_NO, I_E_MARK, BIZ_STAGE_NO, BIZ_STAGE_CODE, BIZ_STAGE_NAME, BIZ_TIME, BIZ_STATUS_CODE, BIZ_STATUS, BIZ_STATUS_DESC, LASTUPDATEDDT, ISDELETED) as DATA\n" +
                "from\n" +
                "  (select bx.BillmsgId, bx.VSL_IMO_NO, bx.VSL_NAME, bx.VOYAGE, bx.ACCURATE_IMONO, bx.ACCURATE_VSLNAME, bx.BL_NO, bx.MASTER_BL_NO, bx.I_E_MARK, bx.BIZ_STAGE_NO, bx.BIZ_STAGE_CODE, bx.BIZ_STAGE_NAME, bx.BIZ_TIME, bx.BIZ_STATUS_CODE, bx.BIZ_STATUS, bx.BIZ_STATUS_DESC, bx.LASTUPDATEDDT, bx.ISDELETED\n" +
                "  from resbill as bx left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF bx.LASTUPDATEDDT as ospd on 'DATA_FLINK_FULL_FLINK_TRACING_LETPAS'=ospd.APP_NAME AND 'DM.TRACK_BIZ_STATUS_BILL'=ospd.TABLE_NAME where ospd.ISCURRENT=1 and bx.BIZ_TIME is not null\n" +
                "  ) as x2\n");

        statementSet.addInsertSql("insert into oracle_track_biz_status_bill\n" +
                "select\n" +
                "  resbill.VSL_IMO_NO, resbill.VSL_NAME, resbill.VOYAGE, resbill.ACCURATE_IMONO, resbill.ACCURATE_VSLNAME,\n" +
                "  resbill.BL_NO, resbill.MASTER_BL_NO, resbill.I_E_MARK, resbill.BIZ_STAGE_NO, resbill.BIZ_STAGE_CODE,\n" +
                "  resbill.BIZ_STAGE_NAME, resbill.BIZ_TIME, resbill.BIZ_STATUS_CODE, resbill.BIZ_STATUS, resbill.BIZ_STATUS_DESC,\n" +
                "  resbill.LASTUPDATEDDT, resbill.ISDELETED\n" +
                "from resbill left join oracle_bill_dim FOR SYSTEM_TIME as OF resbill.LASTUPDATEDDT as obd\n" +
                "  on resbill.VSL_IMO_NO=obd.VSL_IMO_NO\n" +
                "  and resbill.VOYAGE=obd.VOYAGE\n" +
                "  and resbill.BL_NO=obd.BL_NO\n" +
                "  and resbill.BIZ_STAGE_NO=obd.BIZ_STAGE_NO\n" +
                "where (obd.BIZ_TIME is null or resbill.BIZ_TIME>obd.BIZ_TIME) --前者插入后者更新\n" +
                "  and resbill.BIZ_TIME is not null\n");

        statementSet.addInsertSql("insert into oracle_track_biz_status_ctnr\n" +
                "select\n" +
                "  resctn.VSL_IMO_NO,resctn.VSL_NAME,resctn.VOYAGE,resctn.ACCURATE_IMONO,resctn.ACCURATE_VSLNAME,\n" +
                "  resctn.CTNR_NO,resctn.I_E_MARK,resctn.BIZ_STAGE_NO,resctn.BIZ_STAGE_CODE,resctn.BIZ_STAGE_NAME,\n" +
                "  resctn.BIZ_TIME,resctn.BIZ_STATUS_CODE,resctn.BIZ_STATUS,resctn.BIZ_STATUS_DESC,\n" +
                "  resctn.LASTUPDATEDDT,resctn.ISDELETED\n" +
                "from resctn left join oracle_ctnr_dim FOR SYSTEM_TIME as OF resctn.LASTUPDATEDDT as ocd\n" +
                "  on resctn.VSL_IMO_NO=ocd.VSL_IMO_NO\n" +
                "  and resctn.VOYAGE=ocd.VOYAGE\n" +
                "  and resctn.CTNR_NO=ocd.CTNR_NO\n" +
                "  and resctn.BIZ_STAGE_NO=ocd.BIZ_STAGE_NO\n" +
                "where (ocd.BIZ_TIME is null or resctn.BIZ_TIME>ocd.BIZ_TIME)\n" +
                "  and resctn.BIZ_TIME is not null\n");
        statementSet.addInsertSql("insert into kafka_sink1 (GID,APP_NAME,TABLE_NAME,SUBSCRIBE_TYPE,DATA)\n" +
                "select\n" +
                "  CtnrmsgId as GID,\n" +
                "  'DATA_FLINK_FULL_FLINK_TRACING_LETPAS' as APP_NAME,\n" +
                "  'DM.TRACK_BIZ_STATUS_CTNR' as TABLE_NAME,\n" +
                "  'I' as SUBSCRIBE_TYPE,\n" +
                "  ROW(VSL_IMO_NO,VSL_NAME,VOYAGE,ACCURATE_IMONO,ACCURATE_VSLNAME,CTNR_NO,I_E_MARK,BIZ_STAGE_NO,BIZ_STAGE_CODE,BIZ_STAGE_NAME,BIZ_TIME,BIZ_STATUS_CODE,BIZ_STATUS,BIZ_STATUS_DESC,LASTUPDATEDDT,ISDELETED) as DATA\n" +
                "from\n" +
                "  (select CtnrmsgId,cts.VSL_IMO_NO,cts.VSL_NAME,cts.VOYAGE,cts.ACCURATE_IMONO,cts.ACCURATE_VSLNAME,cts.CTNR_NO,cts.I_E_MARK,cts.BIZ_STAGE_NO,cts.BIZ_STAGE_CODE,cts.BIZ_STAGE_NAME,cts.BIZ_TIME,cts.BIZ_STATUS_CODE,cts.BIZ_STATUS,cts.BIZ_STATUS_DESC,cts.LASTUPDATEDDT,cts.ISDELETED\n" +
                "  from resctn as cts left join oracle_subscribe_papam_dim FOR SYSTEM_TIME as OF cts.LASTUPDATEDDT as ospd on 'DATA_FLINK_FULL_FLINK_TRACING_LETPAS'=ospd.APP_NAME AND 'DM.TRACK_BIZ_STATUS_CTNR'=ospd.TABLE_NAME where ospd.ISCURRENT=1 and cts.BIZ_TIME is not null\n" +
                "  ) as x1\n");
        statementSet.execute();
    }
}
