package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyUnionDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> source1 = senv.fromElements(
                new Tuple2<>("Honda", 15),
                new Tuple2<>("CROWN", 25)
        );
        DataStream<Tuple2<String, Integer>> source2 = senv.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40)
        );

        DataStream<Tuple2<String, Integer>> source3 = senv.fromElements(
                new Tuple2<>("Rolls-Royce", 300),
                new Tuple2<>("AMG", 330)
        );

        DataStream<Tuple2<String, Integer>> union = source1.union(source2, source3);
        union.print("union");
        senv.execute();
    }
}
