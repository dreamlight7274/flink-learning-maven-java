package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

public class Tuple {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple1<String>> dataSource = env.fromElements(Tuple1.of("BMW"),
                Tuple1.of("Tesla"), Tuple1.of("Rolls-Royce"));
        DataSet<String> ds = dataSource.map(new MyMapFunction());
        ds.print();
    }                                                             // MapFunction的输入类型和输出类型
    public static class MyMapFunction implements MapFunction<Tuple1<String>, String>{

        @Override
        public String map(Tuple1<String> value) throws Exception{
            return "I like "+value.f0;
        }
    }
}
