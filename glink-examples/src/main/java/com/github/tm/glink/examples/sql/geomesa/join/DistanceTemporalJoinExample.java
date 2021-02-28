package com.github.tm.glink.examples.sql.geomesa.join;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class DistanceTemporalJoinExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_Order (\n" +
                    "id STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE," +
                    "proctime AS PROCTIME())\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = '/home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/join/order.txt',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Geomesa_Warehouse (\n" +
                    "id STRING,\n" +
                    "geom STRING,\n" +
                    "PRIMARY KEY (id) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa',\n" +
                    "  'geomesa.data.store' = 'hbase',\n" +
                    "  'geomesa.schema.name' = 'warehouse_point',\n" +
                    "  'geomesa.spatial.fields' = 'geom:Point',\n" +
                    "  'geomesa.temporal.join.predict' = 'R:400000',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'warehouse_point'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT A.id, A.dtg, B.id " +
            "FROM CSV_Order AS A " +
            "LEFT JOIN Geomesa_Warehouse FOR SYSTEM_TIME AS OF A.proctime AS B " +
            "ON ST_AsText(ST_Point(A.lng, A.lat)) = B.geom");

    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
