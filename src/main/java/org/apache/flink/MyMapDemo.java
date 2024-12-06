package org.apache.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyMapDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);
//        DataStream<Integer> dataStream = env.fromElements(4,1,7).map(new MapFunction<Integer, Integer>() {
//            @Override
//            public Integer map(Integer integer) throws Exception {
//                return integer+8;
//            }
//        });
        DataStream<Integer> dataStream = env.fromElements(4,1,7).map(x->x+8);
        // 为该流print出来的内容添加标签，检查的时候更好分辨
        dataStream.print("Map");
        env.execute("Map job");
    }
}
