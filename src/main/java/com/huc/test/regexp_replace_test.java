package com.huc.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class regexp_replace_test {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		env.setParallelism(1);

//        tEnv.executeSql("select regexp_replace('a   b','[\\n\\t\\r]','')");
		Table table = tEnv.sqlQuery("select trim(regexp_replace('   \t\n    a\tb','[\\n\\t\\r]',''))");
		tEnv.toAppendStream(table, Row.class).print();
//        env.execute();
		Table table1 = tEnv.sqlQuery("select '\t\n    a\tc'");
		tEnv.toAppendStream(table1, Row.class).print();
		
//		Table table2 = tEnv.sqlQuery("select trim(regexp_replace('a  d','[\\' \\']',''))");
//		tEnv.toAppendStream(table2, Row.class).print();
		env.execute();
	}
}
