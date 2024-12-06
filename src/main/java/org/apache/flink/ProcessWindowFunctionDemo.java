package org.apache.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 使用事件时间防止乱序
        sEnv.setParallelism(1);
        DataStream<Tuple2<String, Long>> input = sEnv.fromElements(
                new Tuple2<String, Long>("BMW", 1L),
                new Tuple2<String, Long>("BMW", 2L),
                new Tuple2<String, Long>("Tesla", 3L),
                new Tuple2<String, Long>("BMW", 3L),
                new Tuple2<String, Long>("Tesla", 4L)
        );                                                               //创建升序时间戳提取器，一般会从每个tuple的第二个元素提取时间
        DataStream<String> output = input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                return element.f1;
                // 明确第二个元素是提取时间的元素
            }
            // 设置时间戳（timeStamp）和水印（Watermark）
        })
                .keyBy(t->t.f0)
                // 数据流按照第一个元素分组
                .timeWindow(Time.seconds(1))
                // 四个元素分别是：输入类型，输出类型，分组键的类型（keyBy），窗口的类型（此处是时间窗口）
                .process(new ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        // 窗口的键
                                        ProcessWindowFunction<Tuple2<String, Long>, String, String, TimeWindow>.Context context,
                                        //窗口处理的上下文，其中有许多信息，包括窗口的时间范围（context.window），时间戳，元数据，还可以帮助开发者注册其他事件（在特定时间安排其他操作）
                                        Iterable<Tuple2<String, Long>> input,
                                        // 窗口处理的元素
                                        Collector<String> out
                                        // 输出
                                        ) throws Exception {
                        long count = 0;
                        for (Tuple2<String, Long> in : input) {
                            count++;
                        }
                        out.collect("窗口信息： " + context.window() + "元素数量： " + count);

                    }
                });
        output.print();
        sEnv.execute();


    }
}
// ==================================================================
// 1. sum/max/min 是用来调用简单的聚合方法的
// 2. reduce/aggregate 是用来定义一般的增量聚合函数的（所谓来一个加一个）
// 3. 如果需要设计更复杂的,不仅仅局限于聚合的窗口函数（将数据保存在窗口内，等到触发窗口计算了再统一处理），
// 就用ProcessWindowsFunction和ProcessAllWindowsFunction
// ==================================================================



















