package org.apache.flink;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class AggregationWindowDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Long>> input = sEnv.fromElements(
                new Tuple2<String, Long>("BMW", 2L),
                new Tuple2<String, Long>("BMW", 2L),
                new Tuple2<String, Long>("Tesla", 3L),
                new Tuple2<String, Long>("Tesla", 4L)
        );
        DataStream<Double> output = input.keyBy(0).countWindow(2)
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Tuple2<Long, Long>, Double>() {
                    // 三个参数，第一个表示每个输入参数的类型，第二个表示累加器，用于储存中间计算结果，第三个表示最终输出结果
                    @Override
                    public Tuple2<Long, Long> createAccumulator() {
                        return new Tuple2<Long, Long>(0L, 0L);
                    }
                    // 创造累加器
                    @Override
                    public Tuple2<Long, Long> add(Tuple2<String, Long> value, Tuple2<Long, Long> accumulator){
                        return new Tuple2<>(accumulator.f0+value.f1, accumulator.f1+1L);
                    }
                    // 累加器如何计算
                    @Override
                    public Double getResult(Tuple2<Long, Long> accumulator) {
                        return ((double) accumulator.f0 / accumulator.f1);
                    }
                    // 累加器两格值如何计算最终结果

                    @Override
                    public Tuple2<Long, Long> merge(Tuple2<Long, Long> a, Tuple2<Long, Long> b) {
                        return new Tuple2<>(a.f0+ b.f0, a.f1 + b.f1);
                    }
                    // 如果窗口合并将会调用它， 累加器各个项分别相加


        });

        output.print();
        sEnv.execute();
    }
}


















