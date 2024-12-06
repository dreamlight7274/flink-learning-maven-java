package org.apache.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.expressions.In;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SessionWindowDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.addSource(new MySource());

        DataStream<Tuple2<String, Integer>> output  =input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception{
//                for (String word : sentence.split(" ")){
//                    out.collect(new Tuple2<String, Integer>(word, 1));
//                }
                // 检查来看，这写的有问题，感觉不需要用for loop， 每个MySource都是分隔开的
                out.collect(new Tuple2<String, Integer>(sentence, 1));
            }

                }).keyBy(0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5))).sum(1);
        // 两秒没有新元素产生（或者说没有事件），计算总数
        output.print("window");
        env.execute("WordCount");

    }


    public static class MySource implements SourceFunction<String> {
        private long count = 1L;
        private boolean isRunning = true;
        @Override
        public void run(SourceContext<String> context) throws Exception{
            while (isRunning) {
                List<String> stringList = new ArrayList<>();
                stringList.add("world");
                stringList.add("Flink");
                stringList.add("Stream");
                stringList.add("Batch");
                stringList.add("Table");
                stringList.add("SQL");
                stringList.add("hello");
                int size = stringList.size();
                int i = new Random().nextInt(size);
                context.collect(stringList.get(i));
                // 每次collect都是一个独立事件
                int restTime = i * 1000;
                System.out.println("延迟" + restTime);
                Thread.sleep(restTime);
//                System.out.println(context);

            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}
