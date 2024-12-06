package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyProjectionDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, Double, String>> inPut = env.fromElements(
                new Tuple3<>(1, 2.0, "BMW"),
                new Tuple3<>(2, 2.4, "Tesla")
        );
        DataSet<Tuple2<String, Integer>> out = inPut.project(2,1);
        out.print();
    }
}
