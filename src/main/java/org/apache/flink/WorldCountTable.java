package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class WorldCountTable {
    public static void main(String[] args) throws  Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<ReduceDemo.WC> input = env.fromElements(
                new ReduceDemo.WC("Hello", 1),
                new ReduceDemo.WC("Flink", 1),
                new ReduceDemo.WC("Hello", 1)
        );
        Table table = tEnv.fromDataSet(input);
        Table filtered = table.groupBy(Expressions.$("word")

        ).select(Expressions.$("word"), Expressions.$("frequency").sum().as("frequency"))
                .filter(Expressions.$("frequency").isEqual(2));
        // 转化为指定类型的Dataset
        DataSet<ReduceDemo.WC> result = tEnv.toDataSet(filtered, ReduceDemo.WC.class);
        result.print();
    }
}
