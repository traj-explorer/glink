package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class GeoMesaSQLSourceExample {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a geomesa source table
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
                    "  'hbase.catalog' = 'test-sql'\n" +
                    ")");

    Table result = tEnv.sqlQuery("SELECT * FROM Geomesa_TDrive");
    // print the result to the console
    tEnv.toRetractStream(result, Row.class).print();

    env.execute();
  }
}
