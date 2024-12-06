package org.apache.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyReducedDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> source = senv.fromElements(
                new Tuple2<>("A", 1),
                new Tuple2<>("B", 3),
                new Tuple2<>("C", 6),
                new Tuple2<>("A", 5),
                new Tuple2<>("B", 8)
        );
        // 流处理获得结果后就直接打印，所以当系统获得A,B,C中任何一个时都会打印到控制台上
        DataStream<Tuple2<String, Integer>> reduce = source.keyBy(value->value.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tuple1, Tuple2<String, Integer> tuple2) throws Exception {
                return new Tuple2<>(tuple1.f0, tuple1.f1+tuple2.f1);
            }
        });
        reduce.print("reduce");
        senv.execute("Reduce Demo with different key");
    }
}
