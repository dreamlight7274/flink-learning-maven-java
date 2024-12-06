package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;

import org.apache.flink.table.api.Expressions;

public class StreamForPOJO {
    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<MyOrder> input = env.fromElements(
                new MyOrder(1L,"BMW",1),
                new MyOrder(2L,"Tesla",8),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(3L,"Rolls-Royce",20)
        );

        Table table = tEnv.fromDataSet(input);
        Table filtered = table.where(Expressions.$("amount").isGreaterOrEqual(8));
        // 有的算子是有状态的，需要已被处理过的历史数据或者中间数据。
        DataSet<MyOrder> result = tEnv.toDataSet(filtered, MyOrder.class);
        // 第二个是转换之后的数据集类型
        result.print();

//        env.execute("Flink Streaming Java API Skeleton");
    }

}
