package com.huc.dataStream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, String>> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new Tuple2<>(split[0], split[1]));
            }
        })
                .print();

        env.execute();
    }
}
