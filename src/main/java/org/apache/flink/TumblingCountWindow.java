package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TumblingCountWindow {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("S1", 1),
                Tuple2.of("S1", 2),
                Tuple2.of("S1", 2),
                Tuple2.of("S1", 3),
                // 最后3这个数据将被忽略，因前三个已经被塞入窗口，后面只有单独一个数据，未满3， 应该也有检测方法，flink应该不会直接丢了它
                Tuple2.of("S2", 4),
                Tuple2.of("S2", 5),
                Tuple2.of("S2", 6),
                Tuple2.of("S3", 7),
                Tuple2.of("S3", 8),
                Tuple2.of("S3", 9)
        );
        // 每收集到三个数据计数一次，count window是根据收集到的数据的量来设置
        input.keyBy(0).countWindow(3).sum(1).print();
        env.execute();
    }
}
