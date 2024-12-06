package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

public class WordCountSQL {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);
        DataSet<ReduceDemo.WC> input = env.fromElements(
                new ReduceDemo.WC("Hello", 1),
                new ReduceDemo.WC("Flink", 1),
                new ReduceDemo.WC("Hello", 1)
        );
        // 创建临时视图
        tEnv.createTemporaryView("WordCount", input, Expressions.$("word"), Expressions.$("frequency"));
        Table table = tEnv.sqlQuery(
                "SELECT word, SUM(frequency) as frequency FROM WordCount GROUP BY word"
        );
        // 列名不能用count，会冲突， 将count替换为frequency， count是SQL的保留字段
        // 还是将其转换为对应类型的DataSet
        DataSet<ReduceDemo.WC> result = tEnv.toDataSet(table, ReduceDemo.WC.class);
        result.print();
    }
}


























