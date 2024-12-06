package org.apache.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class TriggerDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<String> input = env.addSource(new SessionWindowDemo.MySource());
        DataStream<Tuple2<String, Integer>> output = input.flatMap(new StreamingJob.Splitter())
                .keyBy(0).timeWindow(Time.seconds(15)).trigger(new MyTrigger()).sum(1);
        output.print("window");  // windowAll会无视键
        env.execute("WordCount");
    }
    public static class MyTrigger extends Trigger {
        int count = 0;
        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext triggerContext)
                throws Exception{
            if (count >5) {
                count = 0;
                System.out.println("Trigger drive");
                return TriggerResult.FIRE;
                // 每当有数据进入时，都会触发onElement触发器，当count>9的时候，count归零
            } else {
                count++;
                System.out.println("onElement : " + element + "Count:"+count);
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext triggerContext) throws Exception{
            return TriggerResult.CONTINUE;
        }
        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE; // 默认行为
        }
        @Override
        public void clear(Window window, TriggerContext triggerContext) throws Exception {
            // 清理窗口状态的逻辑，当窗口被删除时调用，用于清理窗口状态和定时器
//            ValueState<Integer> countState = triggerContext.getPartitionedState(
//                    new ValueStateDescriptor<>("countState", Integer.class, 0));
            // 获取状态
            count = 0;
            System.out.println("clear触发");
//            countState.clear(); // 清理状态
// 虽然窗口被清除时相关数据也会被清除，但是仍然需要编辑clear()使得自定义的状态和资源被按照需求修改和清除
        }
    } // 默认按时间窗口触发被替代


}
// TriggerResult四个行为值，决定窗口的后续行为：
// 1 CONTINUE  不进行任何操作，等待下一个触发条件
// 2 FIRE 窗口计算并输出结果，但窗口状态保持不变
// 3 PURGE 表示不触发窗口计算，只清除窗口内的数据和状态
// 4 FIRE_AND_PURGE 表示触发窗口计算值并输出结果，之后清除清除窗口内的数据状态


























