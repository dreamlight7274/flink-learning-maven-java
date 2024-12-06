package org.apache.flink;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.expressions.In;

public class FirstNTest {
    public static void main(String[] args) throws Exception {
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            DataSet<Tuple2<String, Integer>> in = env.fromElements(
                    new Tuple2<>("BMW", 30),
                    new Tuple2<>("Tesla", 35),
                    new Tuple2<>("Tesla", 55),
                    new Tuple2<>("Tesla", 80),
                    new Tuple2<>("Rolls-Royce", 300),
                    new Tuple2<>("BMW", 40),
                    new Tuple2<>("BMW", 45),
                    new Tuple2<>("BMW", 80)

            );
            DataSet<Tuple2<String, Integer>> out1 = in.first(2);
            DataSet<Tuple2<String, Integer>> out2 = in.groupBy(0).first(2);
            DataSet<Tuple2<String, Integer>> out3 = in.groupBy(0)
                    .sortGroup(1, Order.ASCENDING).first(2);

            out1.print();
            System.out.println("--------------------------");
            out2.print();
            System.out.println("--------------------------");
            out3.print();
    }

}
