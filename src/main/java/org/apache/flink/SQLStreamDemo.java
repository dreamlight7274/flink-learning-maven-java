package org.apache.flink;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.types.Row;


public class SQLStreamDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
        // 流数据的话，这里blink应该可以正常用
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        DataStream<String> stream = env.addSource(new StreamingJob.MySource());
        //将这个流转换为一个表，该表一个列为“word”
        Table tableInter = tEnv.fromDataStream(stream,Expressions.$("word") );
        Table result = tEnv.sqlQuery("SELECT * FROM " + tableInter + " WHERE word LIKE '%t%'");
        // 将表数据再转换为流，并规定格式为row
        tEnv.toAppendStream(result, Row.class).print();

        env.execute();
    }
}
