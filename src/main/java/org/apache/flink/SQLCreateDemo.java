package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;


public class SQLCreateDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        String contents = "" +
                "1,BMW,3,2019-12-12 00:00:01\n" +
                "2,Tesla,4,2019-12-12 00:00:02\n";
        // 创建一个临时csv文件并返回绝对路径
        String path = createTempFile(contents);
        //使用DDL注册表
        String ddl = "CREATE TABLE orders (user_id INT,product STRING,amount INT) " +
                "WITH ('connector.type' = 'filesystem','connector.path' = '" + path + "','format.type' = 'csv')";
        // with跟随表的内容获取途径
        tEnv.executeSql(ddl);
        //在Table上执行SQL查询，并将返回的结果作为新的Table
        String query = "SELECT * FROM orders where product LIKE '%B%'";
        Table result = tEnv.sqlQuery(query);
        // 对已注册的表进行 INSERT 操作
        // 注册 TableSink
        tEnv.executeSql("CREATE TABLE RubberOrders(product STRING, amount INT) WITH ('connector.type' = 'filesystem','connector.path' = 'path','format.type' = 'csv')");
        // 在表上执行 INSERT 语句并向 TableSink 发出结果
        tEnv.executeSql("INSERT INTO RubberOrders SELECT product, amount FROM orders WHERE product LIKE '%B%'");
        //在Table上执行SQL查询，并将返回的结果作为新的Table
        String query2 = "SELECT * FROM RubberOrders";
        Table result2=tEnv.sqlQuery(query2);
        tEnv.toAppendStream(result, Row.class).print();
        tEnv.toAppendStream(result2, Row.class).print();
        //将Table转换为DataStream后，需要执行env.execute()方法来提交Job
        env.execute("Streaming Window SQL Job");
    }
    /**
     * 用contents创建一个临时文件并返回绝对路径。
     */
    private static String createTempFile(String contents) throws IOException {
        File tempFile = File.createTempFile("orders", ".csv");
        tempFile.deleteOnExit();
        // 确保JVM退出时删除临时文件
        FileUtils.writeFileUtf8(tempFile, contents);
        // 使用Apache Commons IO给临时文件写入内容
        return tempFile.toURI().toString();
        // 获得URI（Uniform Resource Identifier）形式的路径，不是简单的文件路径，可以实现网络地址，文件系统路径等
        // 最后将其转换为字符串
    }
}