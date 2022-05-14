package com.huc.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;

import java.util.Properties;

public class MyKafkaUtil {
    private static String KAFKA_SERVER = "192.168.129.121:9092,192.168.129.122:9092,192.168.129.123:9092";
    private static String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";
    private static Properties properties = new Properties();

    static {
        properties.setProperty("bootstrap.servers", KAFKA_SERVER);
    }

    // TODO 将数据发送至Kafka
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), properties);
    }

    // TODO 将数据发送至Kafka 2.0
    public static <T> FlinkKafkaProducer<T> getKafkaSink(KafkaSerializationSchema<T> kafkaSerializationSchema) {
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC,
                kafkaSerializationSchema,
                properties,
                // 当clickhouse不开时下面的默认值就是none
                FlinkKafkaProducer.Semantic.NONE);
    }

    // TODO 从Kafka中获取数据
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {

        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                properties
        );
    }

    //拼接Kafka相关属性到DDL
    public static String getKafkaDDL(String topic, String groupId) {
        return "'connector' = 'kafka', " +
                " 'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + KAFKA_SERVER + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                "  'format' = 'json', " +
                "  'scan.startup.mode' = 'latest-offset'";
    }
}
