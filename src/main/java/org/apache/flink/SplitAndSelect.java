package org.apache.flink;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.expressions.In;

import java.util.ArrayList;
import java.util.List;

public class SplitAndSelect {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv= StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input = senv.fromElements(1,2,3,4,5,6,7,8);
        senv.setParallelism(1);
        SplitStream<Integer> split = input.split(new OutputSelector<Integer>() {
            @Override
            public Iterable<String> select(Integer value) {

                // array 里的数据将决定数据集里的数据被放到哪个标签
                List<String> output = new ArrayList<>();
                if(value % 2 == 0){
                    output.add("even");
                } else {
                    output.add("odd");
                }
                return output;
            }
            //一个子任务就是一个线程
        });
        DataStream<Integer> even = split.select("even");
        DataStream<Integer> odd = split.select("odd");
        DataStream<Integer> all = split.select("even", "odd");
        even.print("Stream even");
        odd.print("Stream odd");
        senv.execute();
    }
}












