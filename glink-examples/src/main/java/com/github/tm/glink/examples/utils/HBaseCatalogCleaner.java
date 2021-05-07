package com.github.tm.glink.examples.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.ipc.Server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Wang Haocheng
 * @date 2021/3/15 - 9:45 下午
 */
public class HBaseCatalogCleaner {
    private static Connection connection;
    private static Configuration configuration;

    public HBaseCatalogCleaner(String zkQuorum) {
        configuration = HBaseConfiguration.create();
        // 如果是集群 则主机名用逗号分隔
        configuration.set("hbase.zookeeper.quorum", zkQuorum);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new HBaseCatalogCleaner("localhost:2181").deleteTable("Xiamen", "Geofence");
    }

    /**
     * GeoMesa表命名格式为 "catalogName_schemaName.*"
     * @param catalogName 目录名
     * @param schemaName 表名
     */
    public void deleteTable(String catalogName, String schemaName) {
        try {
            HBaseAdmin admin = (HBaseAdmin) connection.getAdmin();
            // 删除表前需要先禁用表
            TableName[] tableNames = admin.listTableNames(catalogName + "_" + schemaName + ".*");
            for( TableName tableName : tableNames) {
                HTable table =  new HTable(configuration, tableName);
                HTableDescriptor td = admin.getTableDescriptor(tableName);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                System.out.println(tableName.toString() + " has been deleted;");
                admin.createTable(td);
                System.out.println("New " + tableName.toString() + " has created;");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
