package com.github.tm.glink.examples.demo.crs_transform;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Extract data from csv  files to kafka
 *
 * @author Yu Liebing
 */
public class ExtractToKafka {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    String csvDir = args[0];
    String brokerList = args[1];
    String topic = args[2];

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);
    // csv table
    tEnv.executeSql("CREATE TABLE csv_table (\n" +
            "  pid INT,\n" +
            "  carno STRING,\n" +
            "  lat DOUBLE,\n" +
            "  lng DOUBLE,\n" +
            "  speed DOUBLE,\n" +
            "  azimuth INT,\n" +
            "  `timestamp` BIGINT,\n" +
            "  status INT\n" +
            ") WITH (\n" +
            "  'connector' = 'filesystem',\n" +
            "  'path' = '" + csvDir + "',\n" +
            "  'format' = 'csv'\n" +
            ")");
    // kafka table
    tEnv.executeSql(
            "CREATE TABLE kafka_table (\n" +
                    "  pid INT,\n" +
                    "  carno STRING,\n" +
                    "  lat DOUBLE,\n" +
                    "  lng DOUBLE,\n" +
                    "  speed DOUBLE,\n" +
                    "  azimuth INT,\n" +
                    "  `timestamp` BIGINT,\n" +
                    "  status INT\n" +
                    ") WITH (\n" +
                    " 'connector' = 'kafka',\n" +
                    " 'topic' = '" + topic + "',\n" +
                    " 'properties.bootstrap.servers' = '" + brokerList + "',\n" +
                    " 'properties.group.id' = 'testGroup',\n" +
                    " 'format' = 'csv',\n" +
                    " 'scan.startup.mode' = 'earliest-offset'\n" +
                    ")");

    tEnv.executeSql("INSERT INTO kafka_table (pid, carno, lat, lng, speed, azimuth, `timestamp`, status)" +
                          "SELECT * FROM csv_table");
  }
}
