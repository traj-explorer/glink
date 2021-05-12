package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.sql.GlinkSQLRegister;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Objects;

/**
 * @author Wang Haocheng
 */
public class XiamenGeoFenceInport {

  public static final String ZOOKEEPERS = "localhost:2181";
  public static final String CATALOG_NAME = "Xiamen";
  public static final String GEOFENCE_SCHEMA_NAME = "Geofence";
  public static final String FILEPATH = Objects.requireNonNull(XiamenGeoFenceInport.class.getClassLoader().getResource("XiamenPolygonData.txt")).getPath();

  public static void main(String[] args) {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
    GlinkSerializerRegister.registerSerializer(env);
    GlinkSQLRegister.registerUDF(tEnv);

    // create a source table from csv
    tEnv.executeSql(
            "CREATE TABLE CSV_Area (\n"
                    + "id STRING,\n"
                    + "dtg TIMESTAMP(0),\n"
                    + "geom STRING,\n"
                    + "name STRING)\n"
                    +  "WITH (\n"
                    + "  'connector' = 'filesystem',\n"
                    + "  'path' = '" + FILEPATH + "',\n"
                    + "  'format' = 'csv',\n"
                    + "  'csv.field-delimiter' = ';'\n"
                    + ")");

    // register a table in the catalog
    tEnv.executeSql(
            "CREATE TABLE XiamenGeoFence (\n"
                    + "id STRING,\n"
                    + "dtg TIMESTAMP(0),\n"
                    + "geom STRING,\n"
                    + "name STRING,\n"
//                        "WATERMARK FOR dtg AS dtg,\n"+
                        "PRIMARY KEY (id) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = '" + GEOFENCE_SCHEMA_NAME +  "',\n" +
                        "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                        "  'hbase.zookeepers' = '" + ZOOKEEPERS + "',\n" +
                        "  'hbase.catalog' = '" + CATALOG_NAME + "'\n" +
                        ")");
        // define a dynamic aggregating query
        tEnv.executeSql("INSERT INTO XiamenGeoFence SELECT id, dtg, geom,name FROM CSV_Area");
    }
}
