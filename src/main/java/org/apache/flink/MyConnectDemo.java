package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyConnectDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple1<String>> source1 = senv.fromElements(
                new Tuple1<>("Honda"),
                new Tuple1<>("CROWN")
        );

        DataStream<Tuple2<String, Integer>> source2 = senv.fromElements(
                new Tuple2<>("BMW", 35),
                new Tuple2<>("Tesla", 40)
        );
        ConnectedStreams<Tuple1<String>, Tuple2<String, Integer>> connectedStreams
                = source1.connect(source2);
        // 只是形式上的连接，不能直接打印，还是需要分别get。
        connectedStreams.getFirstInput().print("First input");
        connectedStreams.getSecondInput().print("Second input");
        senv.execute();
    }
}
