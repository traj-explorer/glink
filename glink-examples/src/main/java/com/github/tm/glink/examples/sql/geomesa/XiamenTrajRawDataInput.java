package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.examples.geomesa.GeoMesaHeatMap;
import com.github.tm.glink.examples.source.CSVXiamenTrajectorySource;
import com.github.tm.glink.features.TrajectoryPoint;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;
import java.util.Objects;

public class XiamenTrajRawDataInput {

  @SuppressWarnings("checkstyle:OperatorWrap")
  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);

    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSQLRegister.registerUDF(tEnv);
    String path = Objects.requireNonNull(GeoMesaHeatMap.class.getClassLoader().getResource("XiamenTrajDataCleaned.csv")).getPath();
    // 1,0.0,0,1559805945000,118.178193,24.477954,64d74ff6f332bfaf376572af42152fb8,8199999
    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE XiamenTraj_Example (\n" +
                    "status INTEGER,\n" +
                    "speed DOUBLE,\n" +
                    "azimuth INTEGER,\n" +
                    "intimestamp String,\n" +
                    " lng DOUBLE,\n" +
                    " lat DOUBLE,\n" +
                    " carNo STRING,\n" +
                    " pid STRING,\n" +
                    " pt AS proctime())\n" +
                    "WITH (\n" +
                    "  'connector' = 'filesystem',\n" +
                    "  'path' = '" + path + "',\n" +
                    "  'format' = 'csv'\n" +
                    ")");

    // register a point table in the catalog, with watermark defination
    tEnv.executeSql(
            "CREATE TABLE XiamenTraj_RawData (\n" +
                    "point2 STRING,\n" +
                    "status INTEGER,\n" +
                    "speed DOUBLE,\n" +
                    "azimuth INTEGER,\n" +
                    "times TIMESTAMP(0),\n" +
                    " carNo STRING,\n" +
                    " pid STRING,\n" +
                    " pt AS proctime(),\n" +
                    " PRIMARY KEY (pid) NOT ENFORCED)\n" +
                    "WITH (\n" +
                    "  'connector' = 'geomesa',\n" +
                    "  'geomesa.data.store' = 'hbase',\n" +
                    "  'geomesa.schema.name' = 'Xiamen_Point_et',\n" +
                    "  'geomesa.spatial.fields' = 'point2:Point',\n" +
                    "  'hbase.zookeepers' = 'localhost:2181',\n" +
                    "  'hbase.catalog' = 'Xiamen_Point'\n" +
                    ")");

    // define a dynamic aggregating query
    tEnv.executeSql("INSERT INTO XiamenTraj_RawData SELECT ST_AsText(ST_Point(lng, lat)), status, speed, azimuth, ParseTimestamp(intimestamp),carNo,pid FROM XiamenTraj_Example");
  }
}
