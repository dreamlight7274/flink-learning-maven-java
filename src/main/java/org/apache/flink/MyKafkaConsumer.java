package org.apache.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class MyKafkaConsumer {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // Properties: 用于处理配置文件和键值对存储
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "test", new SimpleStringSchema(), properties);
// 第一个是消费的Kafka主题名称，生产者将消息发布到主题，消费者从主题中订阅消息
        // 第二个参数是告诉系统序列化和反序列化的schema是啥
        // 最后就是kafka的消费者配置
        consumer.setStartFromEarliest();
        // 该方法将会从最早的数据开始获取（包括过期数据）
        DataStream<String> stream = env.addSource(consumer);
        stream.print();
        env.execute();
    }
}




















