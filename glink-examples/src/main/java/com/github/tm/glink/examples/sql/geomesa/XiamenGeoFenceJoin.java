package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.examples.geomesa.GeoMesaHeatMap;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Objects;

/**
 * @author Wang Haocheng
 * @date 2021/3/11 - 9:10 下午
 */
public class XiamenGeoFenceJoin {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GlinkSerializerRegister.registerSerializer(env);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        GlinkSQLRegister.registerUDF(tEnv);
        String rFileName = "XiamenTrajDataCleaned.csv";
        String path = Objects.requireNonNull(GeoMesaHeatMap.class.getClassLoader().getResource(rFileName)).getPath();
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

        // register a geofence table in the catalog, with watermark
        tEnv.executeSql(
                "CREATE TABLE GeoFence (\n" +
                        "id STRING,\n" +
                        "dtg TIMESTAMP(0),\n" +
                        "geom STRING,\n" +
                        "PRIMARY KEY (id) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = 'GeoFence',\n" +
                        "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                        "  'geomesa.temporal.join.predict' = 'I',\n" +
                        "  'hbase.zookeepers' = 'localhost:2181',\n" +
                        "  'hbase.catalog' = 'Xiamen_GeoFence'\n" +
                        ")");

        // define a dynamic aggregating query
        Table result = tEnv.sqlQuery("SELECT * " +
                "FROM XiamenTraj_Example AS A " +
                "LEFT JOIN GeoFence FOR SYSTEM_TIME AS OF A.pt AS B " +
                "ON ST_AsText(ST_Point(A.lng, A.lat)) = B.geom");

        result.printSchema();
        tEnv.toAppendStream(result, Row.class).print();
        env.execute();
    }
}
