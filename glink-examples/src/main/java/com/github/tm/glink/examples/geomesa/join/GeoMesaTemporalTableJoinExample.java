package com.github.tm.glink.examples.geomesa.join;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class GeoMesaTemporalTableJoinExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_Point (\n" +
                    "pid STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE," +
                    "proctime AS PROCTIME())\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = '/home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/join/point.txt',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Geomesa_Area (\n" +
                    "pid STRING,\n" +
                    "dtg TIMESTAMP(0),\n" +
                    "geom STRING,\n" +
                    "PRIMARY KEY (pid) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa',\n" +
                    "  'geomesa.data.store' = 'hbase',\n" +
                    "  'geomesa.schema.name' = 'restricted_area',\n" +
                    "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'restricted_area'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT A.pid, A.dtg, B.pid " +
            "FROM CSV_Point AS A " +
            "LEFT JOIN Geomesa_Area FOR SYSTEM_TIME AS OF A.proctime AS B " +
            "ON ST_AsText(ST_Point(A.lat, A.lng)) = B.geom");

    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
