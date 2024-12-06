package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class ForwardFieldDemo {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple2<Integer, Integer>> input = env.fromElements(Tuple2.of(1,2));
        input.map(new MyMap()).print();

    }
    // FunctionAnnotation:注释声明,优化器将使用此信息。输入元组的f0将映射到输出元组的f1
    @FunctionAnnotation.ForwardedFields("f0->f2")
    public static class MyMap implements MapFunction<Tuple2<Integer, Integer>, Tuple3<String, Integer, Integer>>
    {
        @Override
        public Tuple3<String, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception
        {
            return new Tuple3<String, Integer, Integer>("foo", value.f1*8, value.f0);
        }
    }
}
