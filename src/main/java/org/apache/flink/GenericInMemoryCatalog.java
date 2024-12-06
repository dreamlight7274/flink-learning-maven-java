package org.apache.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;

import java.util.HashMap;
import java.util.List;

import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_TYPE;
import static org.apache.flink.table.descriptors.GenericInMemoryCatalogValidator.CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY;

public class GenericInMemoryCatalog {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tabEnv =
        TableEnvironment.create(EnvironmentSettings.newInstance().build());
// 创建Catalog， 这里是GenericInMemoryCatalog: 适用于测试和开发环境，所有元数据仅在生命周期内可用
        Catalog catalog = new org.apache.flink.table.
                // DEFAULT_DB 是GenericInMemoryCatalog的默认数据库名称，如果不特别指定其他数据库，所有表和视图都将存放在这里
                catalog.GenericInMemoryCatalog(org.apache.flink.table.catalog.GenericInMemoryCatalog.DEFAULT_DB);
        // 其他几类是： HiveCatalog：支持与Hive集成， 使用Hive Metastore来存储和管理Flink表的元数据
        // JdbcCatalog 基于JDBC连接， 支持与各种关系型数据库集成（MySQL, PostgreSQL等）
        // CalciteCatalog: 基于Apache Calcite框架，支持多种类型存储后端，提供对SQL, DDL语句的支持
        // FileSystemCatalog: 基于文件系统，允许将表的元数据以JSON格式存储在本地或分布式系统中。

        // 注册Catalog， 将之前创建的Catalog注册到TableEnvironment， 这样就可以通过tabEnv来访问相关的数据库和表
        tabEnv.registerCatalog("myCatalog", catalog);
        // 创建一个hashMap来储存数据库的表和属性
        HashMap<String, String> hashMap = new HashMap<String, String>();
        // 确定CATALOG的类型（Type）， CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY的对应值就是GenericMemoryCatalog
        hashMap.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_GENERIC_IN_MEMORY);
        // 第二个是描述catalog的版本信息，用于控制版本， 所谓自定义版本号，现在把版本设置为1
        hashMap.put(CATALOG_PROPERTY_VERSION, "1");
        // 通过catalog创建数据库，数据库名为"myDb", 下面就是构建数据库的元数据，comment是对于数据库的描述， 最后false表示如果数据库存在则
        // 覆盖旧数据库，不报错。
        catalog.createDatabase("myDb", new CatalogDatabaseImpl(hashMap, "comment"), false);
        // 创建Catalog表结构schema，帮助快速构建表格
        TableSchema schema = TableSchema.builder()
                .field("name", DataTypes.STRING())
                .field("age", DataTypes.INT())
                .build();
        // 通过schema和hashMap创建表， false意思也是若有重复，覆盖但不报错
        catalog.createTable(
                new ObjectPath("myDb", "mytable"),
                new CatalogTableImpl(schema, hashMap, CATALOG_PROPERTY_VERSION), false
        );
        // 这里也需要版本号
        catalog.createTable(
                new ObjectPath("myDb", "mytable2"),
                new CatalogTableImpl(schema, hashMap, CATALOG_PROPERTY_VERSION), false
        );
        // 用列表存储所有的表的名称
        List<String> tables = catalog.listTables("myDb");
        System.out.println("所有的表： " + tables.toString());
        // 将列表转化为字符串
















    }
}
