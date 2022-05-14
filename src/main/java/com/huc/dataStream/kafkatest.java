package com.huc.dataStream;

import com.huc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class kafkatest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource("data-xpq-db-parse-result", "flink-sql-full-link-tracing-cuschk"));

        streamSource.print();

        env.execute();
    }
}
