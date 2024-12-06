package org.apache.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class GenericInMemoryCatalogDDL {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        HashMap<String, String> hashMap = new HashMap<String, String>();
        hashMap.put(CatalogDescriptorValidator.CATALOG_TYPE,
                GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);
        hashMap.put(CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION, "1");
        Catalog catalog = new GenericInMemoryCatalog(GenericInMemoryCatalog.DEFAULT_DB);
        tableEnv.registerCatalog("mycatalog", catalog);
        // 通过sql语句
        tableEnv.executeSql("CREATE DATABASE myDb");
        tableEnv.executeSql("CREATE TABLE mytable1 (name STRING, age INT)");
        tableEnv.executeSql("CREATE TABLE mytable2 (name STRING, age INT)");
        List<String> tables = Arrays.asList(tableEnv.listTables().clone());
        // listTables将会返回一个包含当前环境所有名称的数组， 使用clone的副本，防止对原数组修改， asList将会把数组转换为一个固定大小的列表
        System.out.println("所有的表" + tables.toString());
        // 列表将会被转换为字符串，并以逗号分割
//        List<String> tables = catalog.listTables("myDb");
//        System.out.println("所有的表： " + tables.toString());
        // 由于操作是针对tableEnv的， 所以表创建在TableEnvironment的默认Catalog中，而不在新注册的mycatalog中，所以无法通过
        // catalog直接找到

    }
}
