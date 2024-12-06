package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TumblingWindowDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new StreamingJob.MySource());
        DataStream<Tuple2<String, Integer>> output = input
                .flatMap(new StreamingJob.Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .sum(1);
        output.print("Tumbling window");
        env.execute("WordCount");
    }
}
