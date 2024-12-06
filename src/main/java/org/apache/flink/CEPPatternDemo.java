package org.apache.flink;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;

public class CEPPatternDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> input = env.fromElements("a1", "c", "b4", "a2", "b2", "a3");
        // <String, ?> 处理的类型是字符串，不限制输出类型       // "start" 模式，会以某个条件开始匹配，下面就是条件
        Pattern<String, ?> pattern = Pattern.<String>begin("start")
                // 设置简单条件
                .where(new SimpleCondition<String>() {
                    @Override
                    public boolean filter(String value) throws Exception {
                        return value.startsWith("a");
                    }
                });
                // 可以用next来连接一个连续策略
//                .next("end").where(new SimpleCondition<String>() {
//                    @Override
//                    public boolean filter(String value) throws Exception {
//                        return value.startsWith("b");
//                    }
//                });
        // 循环模式，循环两次
//        pattern.times(2);
        // 出现三次变为可选，所以可以出现0次或者3次
//        pattern.times(3).optional();
        // 指定期望一个给定事件出现一次或多次
        pattern.oneOrMore();
        // 生成pattern数据流，将定义的模式引用到原数据流中
        PatternStream<String> patternStream = CEP.pattern(input, pattern);
        DataStream<String> result = patternStream.process(
                new PatternProcessFunction<String, String>() {
                    // 用于处理匹配模式，包含了匹配的模式名称及对应的结果列表
                    //接受一个map类参数，上下文，以及collector
                    @Override
                    public void processMatch(Map<String, List<String>> match,
                                             Context context,
                                             Collector<String> out) throws Exception {
                        System.out.println(match);
//                        out.collect(match);


                    }
                }
        );
//        result.print();
        env.execute();



    }
}
