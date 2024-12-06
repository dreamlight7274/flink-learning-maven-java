package org.apache.flink;

import org.apache.flink.api.java.utils.ParameterTool;

import java.util.HashMap;
import java.util.Map;

public class ParameterToolDemo {
    // Parameter tool能帮助用户从Map，配置文件，命令行及系统属性中读取和使用参数
    public static void main(String[] args) throws Exception{
        Map properties = new HashMap<>();
        properties.put("bootstrap.servers", "127.0.0.1:9092");
        properties.put("zookeeper.connect","172.0.0.1:2181");
        properties.put("topic","myTopic");

        ParameterTool parameterTool = ParameterTool.fromMap(properties);
        System.out.println(parameterTool.getRequired("topic"));
        System.out.println(parameterTool.getProperties());

        String propertiesFilePath = "src/main/resources/log4j.properties";
        ParameterTool parameter = ParameterTool.fromPropertiesFile(propertiesFilePath);
        System.out.println(parameter.getProperties());
//        System.out.println(parameter.getRequired("my"));

//        String propertiesFilePath = "src/main/resources/log4j.properties";


    }
}
