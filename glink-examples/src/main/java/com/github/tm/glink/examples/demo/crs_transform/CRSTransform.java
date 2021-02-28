package com.github.tm.glink.examples.demo.crs_transform;

import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author Yu Liebing
 */
public class CRSTransform {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) throws Exception {
    String brokerList = args[0];
    String sourceTopic = args[1];
    String sinkTopic = args[2];

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create an source table
    tEnv.executeSql(
            "CREATE TABLE Points_source (\n" +
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
                    " 'topic' = '" + sourceTopic + "',\n" +
                    " 'properties.bootstrap.servers' = '" + brokerList + "',\n" +
                    " 'properties.group.id' = 'testGroup',\n" +
                    " 'format' = 'csv',\n" +
                    " 'scan.startup.mode' = 'earliest-offset'\n" +
                    ")");
    // create the sink table
    tEnv.executeSql(
            "CREATE TABLE Points_sink (\n" +
                    "  pid INT,\n" +
                    "  carno STRING,\n" +
                    "  wkt STRING,\n" +
                    "  speed DOUBLE,\n" +
                    "  azimuth INT,\n" +
                    "  `timestamp` BIGINT,\n" +
                    "  status INT\n" +
                    ") WITH (\n" +
                    " 'connector' = 'kafka',\n" +
                    " 'topic' = '" + sinkTopic + "',\n" +
                    " 'properties.bootstrap.servers' = '" + brokerList + "',\n" +
                    " 'properties.group.id' = 'testGroup',\n" +
                    " 'format' = 'csv',\n" +
                    " 'scan.startup.mode' = 'earliest-offset'\n" +
                    ")");

    tEnv.executeSql("INSERT INTO Points_sink (pid, carno, wkt, speed, azimuth, `timestamp`, status)" +
                          "SELECT " +
                              "pid, carno, " +
                              "ST_AsText(ST_Transform(ST_Point(lat, lng), 'epsg:4326', 'epsg:3857')) AS wkt, " +
                              "speed, azimuth, `timestamp`, status " +
                          "FROM Points_source");
  }
}
