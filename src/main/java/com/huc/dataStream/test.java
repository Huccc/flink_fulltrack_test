package com.huc.dataStream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        streamSource.map(new MapFunction<String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String,String> map(String s) throws Exception {
                String[] split = s.split(",");
                return new Tuple2<>(split[0],split[1]);
            }
        })
                .print();

        env.execute();
    }
}
