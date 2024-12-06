package org.apache.flink;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FoldWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = sEnv.fromElements(
                new Tuple2<String, Long>("BMW", 2L),
                new Tuple2<String, Long>("BMW", 2L),
                new Tuple2<String, Long>("Tesla", 3L),
                new Tuple2<String, Long>("Tesla", 4L)
        );

        DataStream<String> output= input.keyBy(0).countWindow(2)
                .fold("", new FoldFunction<Tuple2<String, Long>, String>() {
                    @Override
                    public String fold(String accumulator, Tuple2<String, Long> value) throws Exception {
                        return accumulator+ value.f1;
                        // 通过fold将组中的值组合到一个元素中（更像拼接，就像两个2会被拼成22）
                        // 看书上说fold是简单的aggregate
                    }
                });
        output.print();
        sEnv.execute();
    }
}
















