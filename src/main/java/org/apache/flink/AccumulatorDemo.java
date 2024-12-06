package org.apache.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class AccumulatorDemo {
    public static void main(String[] args) throws Exception{
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> input = env.fromElements("BMW", "Tesla","Rolls-Royce");
        DataSet<String> result = input.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                intCounter.add(1);
                return s;
            }
            IntCounter intCounter = new IntCounter();
            @Override
            public void open(Configuration parameters) throws Exception{
                // 确保父类所有逻辑被执行
                super.open(parameters);
                // 将计数器添加到运行上下文
                getRuntimeContext().addAccumulator("myAccumulatorName", intCounter);
            }
        });
        result.writeAsText("E:\\file.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        JobExecutionResult jobExecutionResult = env.execute("myJob");
        int accumulatorResult = jobExecutionResult.getAccumulatorResult("myAccumulatorName");
        System.out.println(accumulatorResult);


    }
}
