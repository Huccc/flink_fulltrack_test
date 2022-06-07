package com.chao.flinksql.departure;

import com.easipass.flink.table.function.udsf.JsonArrayToRowInCOARRI;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class departure_1 {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
		env.setParallelism(1);
		
		tEnv.executeSql("" +
				"CREATE TABLE kafka_source_data (\n" +
				"  msgId STRING,\n" +
				"  bizId STRING,\n" +
				"  msgType STRING,\n" +
				"  bizUniqueId STRING,\n" +
				"  destination STRING,\n" +
				"  parseData STRING,\n" +
				"  LASTUPDATEDDT AS (PROCTIME() + INTERVAL '8' HOUR)\n" +
				") WITH (\n" +
				"  'connector' = 'kafka',\n" +
				"  'topic' = 'topic-bdpcollect-msg-parse-result',\n" +
				"  'properties.bootstrap.servers' = '192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092',\n" +
				"  'properties.group.id' = 'flink-sql-full-link-tracing-coarri-container',\n" +
				"  'format' = 'json',\n" +
				"  'scan.startup.mode' = 'group-offsets'\n" +
				")");

//		Table kafka_source_data = tEnv.sqlQuery("select msgId from kafka_source_data");
//		tEnv.toAppendStream(kafka_source_data, Row.class).print();
//		env.execute();
		
		// TODO ORACLE 提单 dim 维度表
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
				"  'connector' = 'jdbc',\n" +
				"  'url' = 'jdbc:oracle:thin:@192.168.129.149:1521:test12c',\n" +
				"  'table-name' = 'DM.TRACK_BIZ_STATUS_BILL',\n" +
				"  'driver' = 'oracle.jdbc.OracleDriver',\n" +
				"  'username' = 'xqwu',\n" +
				"  'password' = 'easipass'\n" +
				")");
		
		// TODO DIM redis维度表
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
		
		// TODO 注册函数
		tEnv.createTemporarySystemFunction("JSON_STR_TO_ROW_IN_COARRI", JsonArrayToRowInCOARRI.class);
		
		// TODO 解析json串parseData
		tEnv.executeSql("" +
				"create view sourceview as\n" +
				"select\n" +
				"  msgId,bizId,msgType,bizUniqueId,destination,\n" +
				"  JSON_STR_TO_ROW_IN_COARRI(concat('{\"message\":',parseData,'}')).message as Pdata,\n" +
				"  LASTUPDATEDDT,concat(msgId, '^', bizUniqueId, '^', bizId) as GID\n" +
				"from kafka_source_data where bizId='COARRI' and msgType='message_data'");

//		Table sourceview_table = tEnv.sqlQuery("select * from sourceview");
//		tEnv.toAppendStream(sourceview_table, Row.class).print();
//		env.execute();
		
		// TODO 展开pData数组T_T
		tEnv.executeSql("" +
				"create view sourceTB as\n" +
				"select\n" +
				"  msgId,GID,bizId,msgType,bizUniqueId,destination,VesselVoyageInformation,HeadRecord,ContainerInformation,LASTUPDATEDDT\n" +
				"from (select * from sourceview where Pdata is not null) as tempTB1\n" +
				"cross join unnest(Pdata) AS Pdata(HeadRecord,VesselVoyageInformation,TallyCompanyInformation,ContainerInformation,DangerousGoods,RefrigeratedContainer,TailRecord)");

//		Table sourceTB_table = tEnv.sqlQuery("select * from sourceTB");
//		tEnv.toAppendStream(sourceTB_table, Row.class).print();
//		env.execute();
		
		tEnv.executeSql("" +
				"create view test as\n" +
				"select\n" +
				"  dim1.*\n" +
				"from sourceTB\n" +
				"left join oracle_bill_dim FOR SYSTEM_TIME AS OF sourceTB.LASTUPDATEDDT as dim1\n" +
				"on sourceTB.msgId = dim1.VSL_IMO_NO and VSL_NAME = 'JITRA BHUM'");
		
		tEnv.executeSql("" +
				"create view test2 as\n" +
				"select\n" +
//				"  dim1.BIZ_STATUS," +
				"  dim2.VSL_IMO_NO\n" +
				"from sourceTB\n" +
//				"left join oracle_bill_dim FOR SYSTEM_TIME AS OF sourceTB.LASTUPDATEDDT as dim1\n" +
//				"on sourceTB.msgId = dim1.VSL_IMO_NO\n" +
				"left join oracle_bill_dim FOR SYSTEM_TIME AS OF sourceTB.LASTUPDATEDDT as dim2\n" +
				"on sourceTB.destination = dim2.VSL_NAME " +
//				"where dim2.BIZ_STAGE_NAME <> 'N/A'" +
				"");
		
		Table test = tEnv.sqlQuery("select * from test2");
		tEnv.toAppendStream(test, Row.class).print();
		env.execute();
		
		// TODO 提单和箱的公共字段
		tEnv.executeSql("" +
				"create view commnontb as\n" +
				"select temp1.*,\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as VSL_IMO_NO,--在这个逻辑中标准IMO和船舶IMO一样，因为船舶IMO在平文中没有\n" +
				"  if(dim1.res <> '', dim1.res, 'N/A') as ACCURATE_IMONO,\n" +
				"  if(dim2.res <> '', dim2.res, 'N/A') as ACCURATE_VSLNAME,\n" +
				"  if(dim3.res <> '', dim3.res, 'N/A') as BIZ_STAGE_NAME,\n" +
				"  if(dim4.res <> '', dim4.res, 'N/A') as BIZ_STATUS\n" +
				"from\n" +
				"  (select\n" +
				"    --提单和箱的公共字段\n" +
				"    msgId,--用来推送到kafka\n" +
				"    GID,\n" +
				"    if(VesselVoyageInformation.VslName <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.VslName, '[\\t\\n\\r]', ''))), 'N/A') as VSL_NAME,\n" +
				"    if(VesselVoyageInformation.Voyage <> '', UPPER(TRIM(REGEXP_REPLACE(VesselVoyageInformation.Voyage, '[\\t\\n\\r]', ''))), 'N/A') as VOYAGE,\n" +
				"    'N/A' as MASTER_BL_NO,\n" +
				"    VesselVoyageInformation.IEFlag as I_E_MARK,\n" +
				"    if(VesselVoyageInformation.IEFlag = 'E', 'C8.4', 'D3.3') as BIZ_STAGE_NO,\n" +
				"    if(VesselVoyageInformation.IEFlag = 'E', 'E_vslLoading_coarri', 'I_vslUnloading_coarri') as BIZ_STAGE_CODE,\n" +
				"    '1' as BIZ_STATUS_CODE,\n" +
				"    if(HeadRecord.FileDesp is not null, HeadRecord.FileDesp, 'N/A') as BIZ_STATUS_DESC,\n" +
				"    LASTUPDATEDDT,\n" +
				"    0 as ISDELETED,\n" +
				"    ContainerInformation --箱\n" +
				"  from sourceTB where HeadRecord.FileDesp <> 'SBARGE') as temp1\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim1 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp1.VSL_NAME)=dim1.key and 'IMO_NO'=dim1.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim2 on concat('BDCP:DIM:DIM_SHIP:VSL_NAME_EN=',temp1.VSL_NAME)=dim2.key and 'VSL_NAME_EN'=dim2.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim3 on concat('BDCP:DIM:DIM_BIZ_STAGE:SUB_STAGE_NO=',temp1.BIZ_STAGE_NO,'&SUB_STAGE_CODE=',temp1.BIZ_STAGE_CODE)=dim3.key and 'SUB_STAGE_NAME'=dim3.hashkey\n" +
				"left join redis_dim FOR SYSTEM_TIME AS OF temp1.LASTUPDATEDDT as dim4 on concat('BDCP:DIM:DIM_COMMON_MINI:COMMON_CODE=',if(temp1.I_E_MARK ='E', 'load_status', 'discharge_status'),'&TYPE_CODE=',temp1.BIZ_STATUS_CODE)=dim4.key and 'TYPE_NAME'=dim4.hashkey");
		
		Table commnontb_table = tEnv.sqlQuery("select * from commnontb");
		tEnv.toAppendStream(commnontb_table, Row.class).print();
		env.execute();
		
		tEnv.executeSql("" +
				"create view table1 as\n" +
				"select\n" +
				"  oracle_bill_dim.VSL_IMO_NO\n" +
				"from kafka_source_data left join oracle_bill_dim FOR SYSTEM_TIME AS OF kafka_source_data.LASTUPDATEDDT as obcd\n" +
				"  on kafka_source_data.msgId=obcd.VSL_IMO_NO");
		
		Table table1 = tEnv.sqlQuery("select * from table1");
		tEnv.toAppendStream(table1, Row.class).print();
		env.execute();
		
//		tEnv.executeSql("create view table1 as " +
//				"select \n" +
//				" ksd.*\n" +
//				"from kafka_source_data\n" +
//				"left join oracle_bill_dim\n" +
//				"on FOR SYSTEM_TIME AS OF kafka_source_data.LASTUPDATEDDT as ksd\n" +
//				"on kafka_source_data.msgId=ksd.VSL_IMO_NO");
//
//		tEnv.executeSql("select * from table1").print();
	}
}
