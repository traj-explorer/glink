package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class GeoMesaSQLETLExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);

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
                    "`time` TIMESTAMP(0),\n" +
                    "point2 STRING,\n" +
                    "PRIMARY KEY (pid) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa',\n" +
                    "  'geomesa.data.store' = 'hbase',\n" +
                    "  'geomesa.schema.name' = 'geomesa-test',\n" +
                    "  'geomesa.spatial.fields' = 'point2:Point',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'geomesa-test'\n" +
                    ")");

    // define a dynamic aggregating query
    tEnv.executeSql("INSERT INTO Geomesa_TDrive SELECT pid, `time`, ST_AsText(ST_Point(lng, lat)) FROM CSV_TDrive");
  }
}
