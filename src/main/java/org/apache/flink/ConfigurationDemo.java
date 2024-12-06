package org.apache.flink;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class ConfigurationDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Integer> input = env.fromElements(1, 2, 3, 5, 10, 12, 15, 16);
        Configuration configuration = new Configuration();
        configuration.setInteger("limits",8);
        input.filter(new RichFilterFunction<Integer>() {
            private int limit;
            // open方法将在过滤前被调用，查看全局配置中是否有limit，没有则用默认的0。
            @Override
            public void open(Configuration configuration) throws Exception{
                limit = configuration.getInteger("limit",3);
            }
            @Override
            public  boolean filter(Integer value) throws Exception {
                return value > limit;
            }

        }).withParameters(configuration).print();
    }

}
