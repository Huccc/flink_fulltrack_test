package com.huc.flinksql_costrp;

import com.huc.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class costrp_stream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.addSource(MyKafkaUtil.getKafkaSource("source_topic", "source_topic"));

        streamSource.print();

        env.execute();
    }
}
