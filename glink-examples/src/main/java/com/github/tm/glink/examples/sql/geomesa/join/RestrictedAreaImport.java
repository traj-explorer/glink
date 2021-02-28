package com.github.tm.glink.examples.sql.geomesa.join;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RestrictedAreaImport {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_Area (\n" +
                    "id STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "geom STRING)\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = '/home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/join/polygon.txt',\n" +
                    "  'format' = 'csv',\n" +
                    "  'csv.field-delimiter' = ';'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Geomesa_Area (\n" +
                    "id STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "geom STRING,\n" +
                    "PRIMARY KEY (id) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa',\n" +
                    "  'geomesa.data.store' = 'hbase',\n" +
                    "  'geomesa.schema.name' = 'restricted_area',\n" +
                    "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'restricted_area'\n" +
                    ")");

    // define a dynamic aggregating query
    tEnv.executeSql("INSERT INTO Geomesa_Area SELECT id, dtg, geom FROM CSV_Area");
  }
}
