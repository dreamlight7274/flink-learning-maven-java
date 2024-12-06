package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SlideCountWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("S1", 1),
                Tuple2.of("S1", 2),
                Tuple2.of("S1", 3),
                Tuple2.of("S1", 4),
                Tuple2.of("S2", 4),
                Tuple2.of("S2", 5),
                Tuple2.of("S2", 6),
                Tuple2.of("S3", 7),
                Tuple2.of("S3", 8),
                Tuple2.of("S3", 9)
        );
        // 每隔1个数据创造一个新window，每个window装三个数据
        input.keyBy(0).countWindow(3,3).sum(1).print("count 3-1");
        env.execute("count 3 - 1");
    }
}
