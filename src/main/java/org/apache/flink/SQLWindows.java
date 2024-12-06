package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;

public class SQLWindows {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String contents = "1,A,3,2020-11-11 00:00:01\n" +
                "2,B,3,2020-11-11 00:00:02\n" +
                "4,C,3,2020-11-11 00:00:04\n" +
                "6,D,3,2020-11-11 00:00:06\n" +
                "34,A,2,2020-11-11 00:00:34\n" +
                "26,A,2,2020-11-11 00:00:26\n" +
                "8,B,1,2020-11-11 00:00:08";
        String path = createTempFile(contents);
        String ddl = "CREATE TABLE Orders (\n" +
                "order_id INT, \n" +
                "product STRING, \n" +
                "amount INT, \n" +
                "ts TIMESTAMP(3), \n" +
                "WATERMARK FOR ts AS ts - INTERVAL '3' SECOND\n" +
                ") WITH (\n" +
                " 'connector.type' = 'filesystem', \n" +
                " 'connector.path' = '" + path + "', \n" +
                "'format.type' = 'csv' \n" +
                ")";
        tEnv.executeSql(ddl).print();
        tEnv.executeSql("DESCRIBE Orders").print();
        String query = "SELECT\n" +
                "  CAST(TUMBLE_START(ts, INTERVAL '5' SECOND) AS STRING) window_start,\n" +
                // 返回窗口开始时间，并转换为字符串
                "  COUNT(*) order_num,\n" +
                "  SUM(amount) amount_num,\n" +
                "  COUNT(DISTINCT product) products_num\n" +
                "FROM Orders\n" +
                "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";
        // 通过ts来分组，每5秒一个分区
        Table result = tEnv.sqlQuery(query);
        result.printSchema();
        tEnv.toAppendStream(result, Row.class).print();
        env.execute("SQL Job");

    }
    private static String createTempFile(String contents) throws Exception {
        File tempFile = File.createTempFile("Orders", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }

}


















