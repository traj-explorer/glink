package com.github.tm.glink.examples.sql;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author Yu Liebing
 */
public class StreamSpatialSQLExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE Points (\n" +
                    " x DOUBLE,\n" +
                    " y DOUBLE,\n" +
                    " id STRING\n" +
                    ") WITH (\n" +
                    " 'connector' = 'kafka',\n" +
                    " 'topic' = 'Points',\n" +
                    " 'properties.bootstrap.servers' = 'localhost:9092',\n" +
                    " 'properties.group.id' = 'testGroup',\n" +
                    " 'format' = 'csv',\n" +
                    " 'scan.startup.mode' = 'earliest-offset'\n" +
                    ")");

    // define a dynamic aggregating query
    final Table result = tEnv.sqlQuery("SELECT * FROM Points WHERE " +
            "ST_Contains(" +
            "ST_GeomFromText('POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))'), ST_Point(x, y)" +
            ")");

    // print the result to the console
    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
