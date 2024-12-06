package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class BulkIteration {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 从0开始， 最大迭代数10次
        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(1000);
        // iterable
        DataSet<Integer> iteration = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
                return i + ((x * x + y * y < 1)? 1 : 0);
            }
        });
        // 设定终止条件,并获取数据，迭代数据不能直接被使用,必须得用closeWith结束迭代，closeWith会将iteration
        // 10次迭代后的结果合并给initial
        DataSet<Integer> count = initial.closeWith(iteration);
        count.map(new MapFunction<Integer, Double>() {
           @Override
           public Double map(Integer count) throws Exception {
               return  count / (double) 1000*4;
           }
        }).print();
    }
}
