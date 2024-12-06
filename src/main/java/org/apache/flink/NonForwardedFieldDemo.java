package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;

public class NonForwardedFieldDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(
                Tuple2.of(1,2)
        );
        input.map(new MyMap()).print();
    }

    @FunctionAnnotation.NonForwardedFields("f1")  // f1将不会被转发到下游
    public static class MyMap implements MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>{
        @Override
        public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> val) {
            return new Tuple2<Integer, Integer>(val.f0, val.f1*8);
        }
    }
}
















