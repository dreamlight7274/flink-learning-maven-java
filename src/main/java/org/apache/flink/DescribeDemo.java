package org.apache.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class DescribeDemo {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.executeSql(
                "CREATE TABLE Orders (" +
                        "`user` BIGINT NOT NULL," +
                        "product VARCHAR(32)," +
                        "amount INT," +
                        "ts TIMESTAMP(3)," +
                        "ptime AS PROCTIME()," +
                        "PRIMARY KEY(`user`) NOT ENFORCED," +
                        "WATERMARK FOR ts AS ts - INTERVAL '1' SECONDS" +
                        ") "

        );
        tableEnv.executeSql("DESCRIBE Orders").print();




    }
}
