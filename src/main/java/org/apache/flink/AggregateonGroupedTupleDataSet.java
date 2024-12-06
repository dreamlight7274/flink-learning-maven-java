package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple3;

public class AggregateonGroupedTupleDataSet {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> input = env.fromElements(
                new Tuple3<>(1, "a", 1.0),
                new Tuple3<>(2,"b", 2.0),
                new Tuple3<>(4, "b", 4.0),
                new Tuple3<>(3, "c", 3.0)
        );
        DataSet<Tuple3<Integer, String, Double>> output1 = input.groupBy(1).aggregate(Aggregations.SUM,0)
                .and(Aggregations.MIN, 2);
        DataSet<Tuple3<Integer, String, Double>> output2 = input.groupBy(1)
                .aggregate(Aggregations.SUM, 0).aggregate(Aggregations.MIN, 2);
        output1.print();
        System.out.println("--------------------");
        output2.print();











    }
}
