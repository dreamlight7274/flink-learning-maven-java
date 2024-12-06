package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.util.Collector;

public class MyFlatMap {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> dataSource = env.fromElements("Apache Flink is a framework and " +
                "distributed processing engine for stateful computations over unbounded and" +
                "bounded data streams.");
        FlatMapOperator<String, String> flapMap = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> out) throws Exception {
                for (String word: s.split(" ")){
                    out.collect(word);
                }
            }
        });
        flapMap.print();
    }
}
