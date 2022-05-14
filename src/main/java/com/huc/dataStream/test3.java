package com.huc.dataStream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class test3 {
    public static void main(String[] args) throws Exception {
        // 创建一个DataStream
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");

        streamSource.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.split(",")[2].equals("3");
            }
        })
                .print();

        env.execute("test3");
    }
}
