package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.scala.ConnectedStreams;

public class CoGroupDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Integer, String, Double>> car = env.fromElements(
                Tuple3.of(1, "BMW", 20.2),
                Tuple3.of(2, "BMW", 32.1),
                Tuple3.of(3, "Tesla", 21.5),
                Tuple3.of(4, "Tesla", 41.2),
                Tuple3.of(5, "Rolls-Royce", 32.1),
                Tuple3.of(6, "Rolls-Royce", 71.2)
        );
        DataSet<Tuple4<Integer, String, String, Integer>> owner = env.fromElements(
                Tuple4.of(231232, "Dante", "Tesla", 21),
                Tuple4.of(554353, "Dream", "Mercedes Benz", 34),
                Tuple4.of(454323, "Nono", "Audi", 42),
                Tuple4.of(643321, "Jack", "BMW", 51),
                Tuple4.of(324132,"Kevin", "Rolls-Royce", 32)

        );
// problem, cogroup connect
//        DataSet<Tuple2<Tuple3<Integer, String, Double>,Tuple4<Integer, String, String, Integer>>> result
//                = car.coGroup(owner)
//        ConnectedStreams<Tuple3<Integer, String, Double>,Tuple4<Integer, String, String, Integer>> connectedStreams
//                = car.connect(owner);
    }
}
