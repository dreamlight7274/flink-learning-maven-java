package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class CDCDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        String query ="CREATE TABLE orders(" +
                "id BIGINT," +
                "user_id BIGINT," +
                "create_time TIMESTAMP(0)," +
                "payment_way STRING," +
                "delivery_address STRING," +
                "order_comment STRING, " +
                "order_status STRING, " +
                "total_amount DECIMAL(10, 5)) WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = '127.0.0.1'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = 'root'," +

                "'database-name' = 'flink-connect'," +
                "'table-name' = 'orders')";
// 连接器多种多样，这里用的是mysql-cdc
        tEnv.executeSql(query);
        String query2 = "SELECT * FROM orders";
        Table result2 = tEnv.sqlQuery(query2);
        tEnv.toRetractStream(result2, Row.class).print();
        // 将动态表转换为普通流并打印，用row来表示每一行的数据
        env.execute("CDC Job");
    }
}
