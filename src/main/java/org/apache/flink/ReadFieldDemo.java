package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;

public class ReadFieldDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple4<Integer, Integer, Integer, Integer>> input = env.fromElements(Tuple4.of(1,2,3,4));
        input.map(new MyMap()).print();


    }
    // 些声明主要就是告诉Flink优化器哪些要用哪些不要用，这样只有需要的字段会被序列化和反序列化
    @FunctionAnnotation.ReadFields("f0; f3")
    public static class MyMap implements MapFunction<Tuple4<Integer, Integer, Integer, Integer>,
            Tuple2<Integer,Integer>> {
        @Override
        public Tuple2<Integer, Integer> map(Tuple4<Integer, Integer, Integer,Integer> val) {
            if(val.f0 ==2){
                return new Tuple2<Integer, Integer>(val.f0, val.f1);
            } else {
                return new Tuple2<Integer, Integer>(val.f3+8, val.f1+8);
            }
        }
    }
}
