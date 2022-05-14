package com.huc.flinksql_elc300;

import com.easipass.flink.table.function.udsf.JsonToRowInECL300;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class elc300 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
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
				"  'topic' = 'source_topic',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'source_topic',\n" +
				"  'scan.startup.mode' = 'latest-offset',\n" +
				"  'format' = 'json'\n" +
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
				"  'operate.type' = 'hash'\n" +
				")");
		
		// TODO 注册临时函数
		tEnv.createTemporarySystemFunction("parseECL300", JsonToRowInECL300.class);
		
		// TODO 解析ECL300
		tEnv.executeSql("" +
				"create view sourceECL300TB as\n" +
				"select msgId,LASTUPDATEDDT,parseECL300(parseData).DataInfo.DockInfoList as pData\n" +
				"from kafka_source_data where bizId='ECL300' and msgType='message_data'");
		
		// TODO 展开数组
		tEnv.executeSql("" +
				"create view crossTB as\n" +
				"select msgId,LASTUPDATEDDT,DockInfo\n" +
				"from (select * from sourceECL300TB where pData is not null) as tempTB1 cross join unnest(pData) AS pData(DockInfo)");
		
		// TODO 获取公共字段
		tEnv.executeSql("" +
				"create view commonTB as\n" +
				"select \n" +
				"  msgId,\n" +
				"  if(DockInfo.SHIP_CODE <> '', REPLACE(UPPER(TRIM(REGEXP_REPLACE(DockInfo.SHIP_CODE, '[\\t\\n\\r]', ''))),'UN',''), 'N/A') as VSL_IMO_NO,\n" +
				"  if(DockInfo.SHIP_NAME_EN <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.SHIP_NAME_EN, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"  if(DockInfo.VOYAGE <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.VOYAGE, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"  if(DockInfo.BL_NO <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.BL_NO, '[\\t\\n\\r]', ''))), 'N/A') as BL_NO,\n" +
				"  if(DockInfo.CNTR_NO <> '', UPPER(TRIM(REGEXP_REPLACE(DockInfo.CNTR_NO, '[\\t\\n\\r]', ''))), 'N/A') as CTNR_NO,\n" +
				"  TO_TIMESTAMP(DockInfo.OPER_TIME, 'yyyyMMddHHmmss') as BIZ_TIME,\n" +
				"  'N/A' as MASTER_BL_NO,\n" +
				"  DockInfo.STATUS_CODE as BIZ_STATUS_CODE,\n" +
				"  'I' as I_E_MARK,\n" +
				"  'D7.7' as BIZ_STAGE_NO,\n" +
				"  'I_portOut_liftOff' as BIZ_STAGE_CODE,\n" +
				"  'N/A' as BIZ_STATUS_DESC,\n" +
				"  LASTUPDATEDDT,\n" +
				"  0 as ISDELETED\n" +
				"from crossTB");

//		Table commonTB_table = tEnv.sqlQuery("select * from commonTB");
//		tEnv.toAppendStream(commonTB_table, Row.class).print();
//		env.execute();
		
		// TODO 关联维表
		tEnv.executeSql("" +
				"create view withDIMTB as \n" +
				"select \n" +
				"  commonTB.*,\n" +
				"  if(dim1.res <> '', dim1.res, if(dim5.res <> '', dim5.res, 'N/A')) as ACCURATE_IMONO, --标准IMO\n" +
				"  if(dim2.res <> '', dim2.res, if(dim6.res <> '', dim6.res, 'N/A')) as ACCURATE_VSLNAME, --标准船名\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME, --业务节点名称\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS --业务状态\n" +
				"from commonTB\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',commonTB.VSL_IMO_NO) = dim1.key and 'IMO_NO' = dim1.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:IMO_NO=',commonTB.VSL_IMO_NO) = dim2.key and 'VSL_NAME_EN' = dim2.hashkey --通过IMO查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim5 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim5.key and 'IMO_NO' = dim5.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim6 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',commonTB.VSL_NAME) = dim6.key and 'VSL_NAME_EN' = dim6.hashkey --通过船名查\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',commonTB.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',commonTB.BIZ_STAGE_CODE) = dim3.key and 'SUB_STAGE_NAME' = dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF commonTB.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=lift_off_status&TYPE_CODE=',commonTB.BIZ_STATUS_CODE) = dim4.key and 'TYPE_NAME' = dim4.hashkey");
		
		Table withDIMTB_table = tEnv.sqlQuery("select * from withDIMTB");
		tEnv.toAppendStream(withDIMTB_table, Row.class).print();
		env.execute();
		
		StatementSet statementSet = tEnv.createStatementSet();
		
		// TODO Sink
		statementSet.addInsertSql("" +
				"");
		
		statementSet.addInsertSql("" +
				"");
		
		statementSet.addInsertSql("" +
				"");
		
		statementSet.addInsertSql("" +
				"");
		
		statementSet.execute();
	}
}
