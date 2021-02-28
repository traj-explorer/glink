package com.github.tm.glink.examples.hbase;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class HBaseTemporalTableJoin1 {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_TDrive (\n" +
                    "pid STRING,\n" +
                    "`time` TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE)\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = 'file:///home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/1277-reduced.txt',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Geomesa_TDrive (\n" +
                    "pid STRING,\n" +
                    "f ROW<`time` TIMESTAMP(0), point STRING>,\n" +
                    "PRIMARY KEY (pid) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'hbase-1.4',\n" +
                    "  'table-name' = 'table',\n" +
                    "  'zookeeper.quorum' = 'localhost:2181'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT A.pid, B.f " +
            "FROM CSV_TDrive AS A " +
            "LEFT JOIN Geomesa_TDrive FOR SYSTEM_TIME AS OF A.`time` AS B " +
            "ON A.pid = B.pid");
  }
}
