package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import java.io.IOException;
import java.io.File;

public class ExplainDemo {
    public static void main(String[] args) throws IOException{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String contents = "" + "1,BMW,3,2019-12-12 00:00:01\n" +
                "2,Tesla,4,2019-12-12 00:00:02\n";
        String path = createTempFile(contents);
        tEnv.executeSql("CREATE TABLE MyTable (id bigint, word VARCHAR(256)) WITH" +
                "('connector.type' = 'filesystem','connector.path' = 'path', 'format.type' = 'csv')");
        // 通过explainSql()方法解释SQL语句
        String explanation = tEnv.explainSql("SELECT id, word FROM MyTable WHERE word LIKE 'B%'");
        System.out.println(explanation);
        // 在executeSql()方法执行解释SQL语句
        TableResult tableResult = tEnv.executeSql("EXPLAIN PLAN FOR " + "SELECT id, word FROM " +
                "MyTable WHERE word LIKE 'a%' ");
        tableResult.print();

    }

    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("MyTable", ".csv");
        tempFile.deleteOnExit();
        FileUtils.writeFileUtf8(tempFile, contents);
        return tempFile.toURI().toString();
    }
}
