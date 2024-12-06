package org.apache.flink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SideOutputDemo {
    public static void main(String[] args) throws Exception{
        // 两个tag用于表示旁路输出
        final OutputTag<String> outputTag1 = new OutputTag<String>("side-output1"){};
        final OutputTag<String> outputTag2 = new OutputTag<String>("side-output2"){};

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input = env.fromElements(1,2,3,4);
        SingleOutputStreamOperator<Integer> mainDataStream = input
                // 这个是DataSteam的子类，其实就定好output的dataStream
                .process(new ProcessFunction<Integer, Integer>() {
                    @Override
                    public void processElement(Integer value,
                                               ProcessFunction<Integer, Integer>.Context context,
                                               Collector<Integer> out) throws Exception {
                        // 常规输出
                        out.collect(value);
                        // 发送输出到旁路输出
                        context.output(outputTag1, "sideout1-" + String.valueOf(value));
                        context.output(outputTag2, "sideout2-" + String.valueOf(value*3));

                    }
                });
        DataStream<String> sideOutputStream1 = mainDataStream.getSideOutput(outputTag1);
        DataStream<String> sideOutputStream2 = mainDataStream.getSideOutput(outputTag2);


        sideOutputStream1.print("sideOutputStream-1");
        sideOutputStream2.print("sideOutputStream-2");
        mainDataStream.print("originalOutput");
        env.execute();
    }
}





















