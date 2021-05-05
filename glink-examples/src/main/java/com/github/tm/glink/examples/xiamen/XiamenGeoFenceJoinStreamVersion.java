package com.github.tm.glink.examples.xiamen;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.sink.GeoMesaSinkFunction;
import com.github.tm.glink.connector.geomesa.sink.PointToSimpleFeatureConverter;
import com.github.tm.glink.connector.geomesa.util.GeoMesaTableSchema;
import com.github.tm.glink.connector.geomesa.util.GeoMesaType;
import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.core.source.CSVStringSourceSimulation;
import com.github.tm.glink.sql.GlinkSQLRegister;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Wang Haocheng
 * @date 2021/3/11 - 9:10 下午
 */
public class XiamenGeoFenceJoinStreamVersion {
    public static final String ZK_QUORUM = "localhost:2181";
    public static final String CATALOG_NAME = "Xiamen";
    public static final String POINTS_SCHEMA_NAME = "TrajectoryPointsStreamVersion";
    public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
    public static final int TIMEFIELDINDEX = 3;
    public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
    public static final int SPEED_UP = 1;
    public static final int PARALLELSIM = 1;

    public static void main(String[] args) throws Exception {
        // 一些配置变量
        Configuration confForPolygon = new Configuration();
        confForPolygon.setString("geomesa.schema.name", "Geofence");
        confForPolygon.setString("geomesa.spatial.fields", "geom:Polygon");
        confForPolygon.setString("hbase.zookeepers", "localhost:2181");
        confForPolygon.setString("geomesa.data.store", "hbase");
        confForPolygon.setString("hbase.catalog", "Xiamen");
        confForPolygon.setString("geomesa.primary.field.name", "pid");

        List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForPolygon = new LinkedList<>();
        fieldNamesToTypesForPolygon.add(new Tuple2<>("id", GeoMesaType.STRING));
        fieldNamesToTypesForPolygon.add(new Tuple2<>("dtg", GeoMesaType.DATE));
        fieldNamesToTypesForPolygon.add(new Tuple2<>("geom", GeoMesaType.POLYGON));
        fieldNamesToTypesForPolygon.add(new Tuple2<>("name", GeoMesaType.STRING));

        Configuration confForOutputPoints = new Configuration();
        confForOutputPoints.setString("geomesa.schema.name", POINTS_SCHEMA_NAME);
        confForOutputPoints.setString("geomesa.spatial.fields", "point2:Point");
        confForOutputPoints.setString("hbase.zookeepers", ZK_QUORUM);
        confForOutputPoints.setString("geomesa.data.store", "hbase");
        confForOutputPoints.setString("hbase.catalog", CATALOG_NAME);
        confForOutputPoints.setString("geomesa.primary.field.name", "pid");

        List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForPoints = new LinkedList<>();
        fieldNamesToTypesForPoints.add(new Tuple2<>("point2", GeoMesaType.POINT));
        fieldNamesToTypesForPoints.add(new Tuple2<>("status", GeoMesaType.INTEGER));
        fieldNamesToTypesForPoints.add(new Tuple2<>("speed", GeoMesaType.DOUBLE));
        fieldNamesToTypesForPoints.add(new Tuple2<>("azimuth", GeoMesaType.INTEGER));
        fieldNamesToTypesForPoints.add(new Tuple2<>("ts", GeoMesaType.DATE));
        fieldNamesToTypesForPoints.add(new Tuple2<>("tid", GeoMesaType.STRING));
        fieldNamesToTypesForPoints.add(new Tuple2<>("pid", GeoMesaType.STRING));
        fieldNamesToTypesForPoints.add(new Tuple2<>("fid", GeoMesaType.STRING));
        GeoMesaTableSchema geoMesaTableSchema = GeoMesaTableSchema.fromFieldNamesAndTypes(fieldNamesToTypesForPoints , confForOutputPoints);
        GeoMesaDataStoreParam geoMesaDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
        geoMesaDataStoreParam.initFromConfigOptions(confForOutputPoints);

        // 将原Join后的表删除
//        new HBaseCatalogCleaner(ZK_QUORUM).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        GlinkSerializerRegister.registerSerializer(env);
        GlinkSQLRegister.registerUDF(tEnv);
        env.getConfig().setAutoWatermarkInterval(1000L);
        env.setParallelism(PARALLELSIM);


        BroadcastSpatialDataStream bsd = new BroadcastSpatialDataStream<>(env, "Geomesa-HBase", fieldNamesToTypesForPolygon, confForPolygon);
        SpatialDataStream<Point> originalDataStream = new SpatialDataStream<Point>(
                env, new CSVStringSourceSimulation(FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false),
                4, 5, TextFileSplitter.CSV, GeometryType.POINT, true,
                Schema.types(Integer.class, Double.class, Integer.class, Long.class, String.class, String.class))
                .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> ((Tuple) event.getUserData()).getField(TIMEFIELDINDEX))));
        originalDataStream.spatialDimensionJoin(bsd, TopologyType.N_CONTAINS, new addFenceId(), new TypeHint<Point>(){}).addSink(
                new GeoMesaSinkFunction<Point>(geoMesaDataStoreParam, geoMesaTableSchema,
                        new PointToSimpleFeatureConverter(geoMesaTableSchema)));
        env.execute();
    }


    private static class addFenceId implements JoinFunction<Point, Polygon, Point> {
        @Override
        public Point join(Point first, Polygon second) throws Exception {
            String fid = ((Tuple)second.getUserData()).getField(0);
            Tuple oldUserData = (Tuple)first.getUserData();
            Tuple newUserData = Tuple.newInstance(oldUserData.getArity() + 1);
            for (int i = 0; i < oldUserData.getArity(); i++) {
                newUserData.setField(oldUserData.getField(i), i);
            }
            newUserData.setField(fid, newUserData.getArity()-1);
            first.setUserData(newUserData);
            return first;
        }
    }
}
