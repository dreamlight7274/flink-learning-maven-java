package org.apache.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.expressions.In;

public class MyFilter {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(-1, -2, -3, 1, 2, 417);
//        DataSet<Integer> ds = input.filter(new FilterFunction<Integer>() {
//            @Override
//            public boolean filter(Integer integer) throws Exception {
//                return integer>0;
//            }
//        });
        //----------------------------
        DataSet<Integer> ds = input.filter(new MyFilterFunction());


        ds.print();
    }
//    public static class MyFilterFunction implements FilterFunction<Integer>{
//        @Override
//        public boolean filter(Integer value) throws Exception{
//            return value>0;
//        }
//    }
    public static class MyFilterFunction extends RichFilterFunction<Integer>{
        @Override
        public boolean filter(Integer value) throws Exception{
            return value>0;
        }
}
}
