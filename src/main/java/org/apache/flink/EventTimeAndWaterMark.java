package org.apache.flink;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;

public class EventTimeAndWaterMark {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        sEnv.setParallelism(1);
        DataStream<String> input = sEnv.addSource(new MySource());
        DataStream<Tuple2<String, Long>> inputMap = input.map(new MapFunction<String, Tuple2<String, Long>>() {
            // 数据组第一位用String，第二位用long展示时间
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] arr = value.split(",");
                return new Tuple2<>(arr[0], Long.parseLong(arr[1]));
            }
        });
        DataStream<Tuple2<String, Long>> waterMarkStream = inputMap.
        assignTimestampsAndWatermarks(new WatermarkStrategy<Tuple2<String, Long>>() {
            @Override
            public WatermarkGenerator<Tuple2<String, Long>>
            createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimestamp;
                    private long delay = 3000;
                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimeStamp, WatermarkOutput output) {
                        maxTimestamp = Math.max(maxTimestamp, event.f1);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        output.emitWatermark(new Watermark(maxTimestamp - delay));

                    }
                };
            }
        });

        waterMarkStream.process(new ProcessFunction<Tuple2<String, Long>, Object>() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            // yyyy 年， MM 月， dd 日期， HH 24小时制的小时， mm 分钟， ss 秒， SSS 毫秒
// 通过processElement访问在context（上下文）中的水位线
            @Override
            public void processElement(Tuple2<String, Long> value, Context context, Collector<Object> out)
                    throws Exception {
                long w = context.timerService().currentWatermark();
                System.out.println("水位线： " + w + "水位线时间" + sdf.format(w)
                + "消息的事件时间" + sdf.format(value.f1)
                );
            }

        });
        waterMarkStream.print();
        sEnv.execute();
    }

    public static class MySource implements SourceFunction<String> {
        private long count = 1L;
        private boolean isRunning = true;
        // 通过循环生成数据
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception{
            while (isRunning) {
                sourceContext.collect("消息" + count + "," + System.currentTimeMillis());
                count += 1;
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}














