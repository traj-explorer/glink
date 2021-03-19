package com.github.tm.glink.examples.sql.geomesa;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Objects;

/**
 * @author Wang Haocheng
 * @date 2021/3/11 - 7:38 下午
 */
public class XiamenGeoFenceInport {
    @SuppressWarnings("checkstyle:OperatorWrap")
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        GlinkSerializerRegister.registerSerializer(env);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        GlinkSQLRegister.registerUDF(tEnv);
        String path = Objects.requireNonNull(XiamenGeoFenceInport.class.getClassLoader().getResource("XiamenPolygonData.txt")).getPath();

        // create a source table from csv
        tEnv.executeSql(
                "CREATE TABLE CSV_Area (\n" +
                        "id STRING,\n" +
                        "dtg TIMESTAMP(0),\n" +
                        "geom STRING)\n" +
                        "WITH (\n" +
                        "  'connector' = 'filesystem',\n" +
                        "  'path' = '" +path + "',\n" +
                        "  'format' = 'csv',\n" +
                        "  'csv.field-delimiter' = ';'\n" +
                        ")");

        // register a table in the catalog
        tEnv.executeSql(
                "CREATE TABLE XiamenGeoFence (\n" +
                        "id STRING,\n" +
                        "dtg TIMESTAMP(0),\n" +
                        "geom STRING,\n" +
//                        "WATERMARK FOR dtg AS dtg,\n"+
                        "PRIMARY KEY (id) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = 'GeoFence',\n" +
                        "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                        "  'hbase.zookeepers' = 'localhost:2181',\n" +
                        "  'hbase.catalog' = 'Xiamen_GeoFence'\n" +
                        ")");
        // define a dynamic aggregating query
        tEnv.executeSql("INSERT INTO XiamenGeoFence SELECT id, dtg, geom FROM CSV_Area");
    }
}
