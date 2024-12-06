package org.apache.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class ReadCSV {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<User> inputData = env.readCsvFile("E:\\大数据实验\\测试文件\\people.csv")
                .fieldDelimiter(",") //设置分隔符
                .ignoreFirstLine() //忽略第一行
                .includeFields(true, false, true) //选第一行和第三行，忽略第二行
                .pojoType(User.class, "name"
                        , "age"); //用POJO类应用，对应的列是name和age
        inputData.print();

    }
}

















