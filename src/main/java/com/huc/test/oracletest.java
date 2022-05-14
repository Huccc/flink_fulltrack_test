package com.huc.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class oracletest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        env.setParallelism(1);

        tEnv.executeSql("" +
                "CREATE TABLE ORACLE_TRACK_BIZ_STATUS_BILL(\n" +
                "    BL_NO STRING,\n" +
                "    MASTER_BL_NO STRING,\n" +
                "    VSL_IMO_NO STRING,\n" +
                "    VSL_NAME STRING,\n" +
                "    VOYAGE STRING,\n" +
                "    ACCURATE_IMONO STRING,\n" +
                "    ACCURATE_VSLNAME STRING,\n" +
                "    I_E_MARK STRING,\n" +
                "    BIZ_STAGE_NO STRING,\n" +
                "    BIZ_STAGE_CODE STRING,\n" +
                "    BIZ_STAGE_NAME STRING,\n" +
                "    BIZ_TIME TIMESTAMP(3),\n" +
                "    BIZ_STATUS_CODE STRING,\n" +
                "    BIZ_STATUS STRING,\n" +
                "    BIZ_STATUS_DESC STRING,\n" +
                "    LASTUPDATEDDT TIMESTAMP(3),\n" +
                "    ISDELETED DECIMAL(1, 0), -- Oracle NUMBER(1, 0) <-> Flink DECIMAL(1, 0)\n" +
                "    PRIMARY KEY(VSL_IMO_NO, VOYAGE, BIZ_STAGE_NO, BL_NO, BIZ_STATUS_CODE) NOT ENFORCED -- 测试数据的主键全部，这将导致 Job 宕掉\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
                "    'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
                "    'driver' = 'oracle.jdbc.OracleDriver', -- oracle.jdbc.driver.OracleDriver\n" +
                "    'username' = 'dm',\n" +
                "    'password' = 'easipass'\n" +
                ")\n");

        StatementSet statementSet = tEnv.createStatementSet();

        statementSet.addInsertSql("" +
                "INSERT INTO ORACLE_TRACK_BIZ_STATUS_BILL (\n" +
                "  BL_NO,\n" +
                "  MASTER_BL_NO,\n" +
                "  VSL_IMO_NO,\n" +
                "  VSL_NAME,\n" +
                "  VOYAGE,\n" +
                "  ACCURATE_IMONO,\n" +
                "  ACCURATE_VSLNAME,\n" +
                "  I_E_MARK,\n" +
                "  BIZ_STAGE_NO,\n" +
                "  BIZ_STAGE_CODE,\n" +
                "  BIZ_STAGE_NAME,\n" +
                "  BIZ_TIME,\n" +
                "  BIZ_STATUS_CODE,\n" +
                "  BIZ_STATUS,\n" +
                "  BIZ_STATUS_DESC,\n" +
                "  LASTUPDATEDDT,\n" +
                "  ISDELETED\n" +
                ")\n" +
                "SELECT\n" +
                "  '10001',\n" +
                "  'BLOO2022001',\n" +
                "  '9302073',\n" +
                "  'MSC MAEVA',\n" +
                "  'E7',\n" +
                "  '9302073',\n" +
                "  'MSC MAEVA',\n" +
                "  'E',\n" +
                "  'D6.1',\n" +
                "  'E_test',\n" +
                "  'test',\n" +
                "  '2022-04-07 00:00:00',\n" +
                "  'test',\n" +
                "  'test',\n" +
                "  'test',\n" +
                "  '2022-04-07 00:00:20',\n" +
                "  0\n");

        statementSet.execute();

    }
}
