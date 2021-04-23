package com.github.tm.glink.examples.xiamen;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.core.source.CSVStringSourceSimulation;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import com.github.tm.glink.sql.Adapter;
import com.github.tm.glink.sql.GlinkSQLRegister;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TemporalTableFunction;
import org.locationtech.jts.geom.Point;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author Wang Haocheng
 * @date 2021/3/11 - 9:10 下午
 */
public class XiamenGeoFenceJoin {
    public static final String ZK_QUORUM= "localhost:2181";
    public static final String CATALOG_NAME = "Xiamen";
    public static final String POINTS_SCHEMA_NAME = "TrajectoryPoints";
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final int TIMEFIELDINDEX = 3;
    public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
    public static final int SPEED_UP = 50;
    public static final int PARALLELSIM = 1;

    public static void main(String[] args) throws Exception {
        // 将原Join后的表删除
        new HBaseCatalogCleaner(ZK_QUORUM).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        GlinkSerializerRegister.registerSerializer(env);
        GlinkSQLRegister.registerUDF(tEnv);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(PARALLELSIM);

        // 注册相关的表
        registRelatedTables(tEnv);

        SpatialDataStream<Point> trajDataStream = new SpatialDataStream<Point>(
                env, new CSVStringSourceSimulation(FILEPATH,SPEED_UP, TIMEFIELDINDEX, SPLITTER, true),
                4, 5, TextFileSplitter.CSV, GeometryType.POINT, true,
                Schema.types(Integer.class, Double.class,Integer.class,Long.class,String.class,String.class))
                .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner((event,timestamp)->((Tuple)event.getUserData()).getField(TIMEFIELDINDEX))));
        Table rawDataTable = Adapter.toTable(tEnv, trajDataStream,
                Schema.names(true,"point","status","speed","azimuth","rawtime","tid","pid"),
                Schema.types(Point.class, Integer.class, Double.class,Integer.class,Long.class,String.class,String.class));
        // 设置处理时间
        TemporalTableFunction function = rawDataTable.createTemporalTableFunction($("rawtime"),$("pid"));

        // 点-面Join
        Table res = tEnv.sqlQuery("SELECT * " +
                "FROM "+ rawDataTable + " AS A " +
                "LEFT JOIN GeoFence FOR SYSTEM_TIME AS OF A.pt  AS B " +
                "ON ST_AsText(A.point) = B.geom");
        tEnv.executeSql("INSERT INTO PointAfterJoin " +
                "SELECT ST_AsText(point), status, speed, azimuth, ParseTimestamp(rawtime), tid, pid, pt, id " +
                "FROM " + res);

        env.execute();
    }

    /**
     * 需要注册2张表：
     * - 已经在GeoMesa HBase中的围栏表
     * - join后的导出表
     */
    private static void registRelatedTables(StreamTableEnvironment tEnv) {

        tEnv.executeSql(
                "CREATE TABLE GeoFence (\n" +
                        "id STRING,\n" +
                        "dtg TIMESTAMP(0),\n" +
                        "geom STRING,\n" +
                        "name STRING,\n" +
                        "PRIMARY KEY (id) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = '" + XiamenGeoFenceInport.GEOFENCE_SCHEMA_NAME + "',\n" +
                        "  'geomesa.spatial.fields' = 'geom:Polygon',\n" +
                        "  'geomesa.temporal.join.predict' = 'I',\n" +
                        "  'hbase.zookeepers' = '" + ZK_QUORUM + "',\n" +
                        "  'hbase.catalog' = '" + CATALOG_NAME + "'\n" +
                        ")");


        tEnv.executeSql(
                "CREATE TABLE PointAfterJoin" +
                        " (\n" +
                        "point2 STRING,\n" +
                        "status INTEGER,\n" +
                        "speed DOUBLE,\n" +
                        "azimuth INTEGER,\n" +
                        "ts TIMESTAMP(0),\n" +
                        " tid STRING,\n" +
                        " pid STRING,\n" +
                        " pt TIMESTAMP(0),\n" +
                        " fid  String,\n" +
                        " PRIMARY KEY (pid) NOT ENFORCED)\n" +
                        "WITH (\n" +
                        "  'connector' = 'geomesa',\n" +
                        "  'geomesa.data.store' = 'hbase',\n" +
                        "  'geomesa.schema.name' = '" + POINTS_SCHEMA_NAME + "',\n" +
                        "  'geomesa.spatial.fields' = 'point2:Point',\n" +
                        "  'hbase.zookeepers' = '"+ ZK_QUORUM +"',\n" +
                        "  'hbase.catalog' = '"+ CATALOG_NAME + "'\n" +
                        ")");
    }
}
