package org.apache.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class MyKafkaProducer {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //设置数据源
        DataStreamSource<String> dataStreamSource = env.addSource(new MySource());
        // java的properties类， 用来储存kafka的配置属性
        Properties properties = new Properties();
        // 指明broker地址
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // 创建kafka生产者，主题是test，序列化方式是将字符串直接发送，最后一个是Kafka的配置属性
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("test",
                new SimpleStringSchema(), properties);
        // 在发送消息时将事件时间的时间戳写入Kafka，消费者可以获取事件时间
        producer.setWriteTimestampToKafka(true);
        // 将生产者作为接收器添加到数据源。
        dataStreamSource.addSink(producer);
        env.execute();

    }
}
