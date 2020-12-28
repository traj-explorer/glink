package com.github.tm.glink.examples.visualization;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class TileSQLExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_TDrive (\n" +
                    "pid INT,\n" +
                    "`time` TIMESTAMP(3),\n" +
                    "lng DOUBLE,\n" +
                    "lat DOUBLE," +
                    "WATERMARK FOR `time` AS `time` - INTERVAL '5' SECOND)\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = 'file:///home/liebing/Code/javaworkspace/glink/glink-examples/src/main/resources/1277-reduced.txt',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // define a dynamic aggregating query
    final Table result = tEnv.sqlQuery("SELECT " +
            "TUMBLE_START(`time`, INTERVAL '5' SECOND) AS window_start, " +
            "ST_Tile(lat, lng, 12) AS tile, ST_Test(lat, lng) FROM CSV_TDrive " +
            "GROUP BY TUMBLE(`time`, INTERVAL '5' SECOND), ST_Tile(lat, lng, 12)");

    // print the result to the console
    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
