package org.apache.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;

public class ReduceFunctionDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<String, Integer, Double>> tuples = env.fromElements(
                Tuple3.of("BMW" ,30, 2.0),
                Tuple3.of("Tesla", 30, 2.0),
                Tuple3.of("Tesla", 30, 2.0),
                Tuple3.of("Rolls-Royce", 300, 4.0)
        );
        DataSet<Tuple3<String, Integer, Double>> reducedTuples = tuples.groupBy(0)
                .reduce(new ReduceFunction<Tuple3<String, Integer, Double>>() {
                    @Override
                    public Tuple3<String, Integer, Double> reduce(Tuple3<String, Integer, Double> stringIntegerDoubleTuple3, Tuple3<String, Integer, Double> t1) throws Exception {
                        return Tuple3.of(stringIntegerDoubleTuple3.f0, stringIntegerDoubleTuple3.f1+t1.f1, stringIntegerDoubleTuple3.f2+t1.f2);
                    }
                });
        reducedTuples.print();
    }
}
