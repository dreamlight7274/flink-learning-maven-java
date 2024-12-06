package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import org.apache.flink.table.api.Expressions;
import org.apache.flink.types.Row;

public class SQLBatchDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<MyOrder> input = env.fromElements(
                new MyOrder(1L, "BMW", 1),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(2L, "Tesla", 8),
                new MyOrder(3L, "Rolls-Royce", 20)
        );
        //将Dataset转换为临时视图 MyOrder是临时视图的名字， input是数据源，后面用Expressions引用数据源中的某一列
        tEnv.createTemporaryView("MyOrder", input, Expressions.$("id"),
                Expressions.$("product"),
                Expressions.$("amount"));

        Table table = tEnv.sqlQuery("SELECT MIN(id) as id, product, SUM(amount) as amount FROM MyOrder GROUP BY product");

        tEnv.toDataSet(table, Row.class).print();
    }

}
