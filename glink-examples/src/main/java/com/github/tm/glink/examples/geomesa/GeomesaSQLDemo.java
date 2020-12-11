package com.github.tm.glink.examples.geomesa;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class GeomesaSQLDemo {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_TDrive (\n" +
                    "pid INT,\n" +
                    "`time` TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE)\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = 'file:///home/liebing/Code/javaworkspace/glink/glink-connector/glink-connector-geomesa/src/main/resources/1277-reduced.txt',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Geomesa_TDrive (\n" +
                    "pid INT,\n" +
                    "`time` TIMESTAMP(0),\n" +
                    "point2 STRING)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa-hbase',\n" +
                    "  'hbase.catalog' = 'test-sql'\n" +
                    ")");

    // define a dynamic aggregating query
    tEnv.executeSql("INSERT INTO Geomesa_TDrive SELECT pid, `time`, ST_AsText(ST_Point(lat, lng)) FROM CSV_TDrive");

    // print the result to the console
//    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
