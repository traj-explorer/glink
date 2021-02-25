package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class GeoMesaSQLJoinExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_TDrive (\n" +
                    "pid STRING,\n" +
                    "`time` TIMESTAMP(0),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE," +
                    "proctime AS PROCTIME())\n" +
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
                    "  'geomesa.temporal.join.predict' = '-C',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'test-sql'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT A.pid, B.point2 " +
            "FROM CSV_TDrive AS A " +
            "LEFT JOIN Geomesa_TDrive FOR SYSTEM_TIME AS OF A.proctime AS B " +
            "ON ST_AsText(ST_Point(A.lat, A.lng)) = B.point2");

    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
