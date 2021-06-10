package com.github.tm.glink.examples.demo.xiamen;

import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParam;
import com.github.tm.glink.connector.geomesa.options.param.GeoMesaDataStoreParamFactory;
import com.github.tm.glink.connector.geomesa.sink.GeoMesaSinkFunction;
import com.github.tm.glink.connector.geomesa.sink.PointToSimpleFeatureConverter;
import com.github.tm.glink.connector.geomesa.source.GeoMesaSourceFunction;
import com.github.tm.glink.connector.geomesa.source.SimpleFeatureToGeometryConverter;
import com.github.tm.glink.connector.geomesa.util.GeoMesaStreamTableSchema;
import com.github.tm.glink.connector.geomesa.util.GeoMesaType;
import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.datastream.TileDataStream;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.tile.Pixel;
import com.github.tm.glink.core.tile.PixelResult;
import com.github.tm.glink.core.tile.TileResult;
import com.github.tm.glink.examples.utils.HBaseCatalogCleaner;
import com.github.tm.glink.sql.util.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

import java.time.Duration;
import java.util.*;

public class XiamenHeatMap {

  // For spatial data stream source.
  public static final String ZOOKEEPERS = "localhost:2181";
  public static final String CATALOG_NAME = "Xiamen";
  public static final String TILE_SCHEMA_NAME = "Heatmap";
  public static final String POINTS_SCHEMA_NAME = "JoinedPoints";
  public static final String FILEPATH = "/Users/haocheng/Code/glink/glink-examples/src/main/resources/XiamenTrajDataCleaned.csv";
  public static final int SPEED_UP = 50;
  public static final long WIN_LEN = 10L;
  public static final int PARALLELISM = 20;
  public static final int CARNO_FIELD_UDINDEX = 4;
  public static final int TIMEFIELDINDEX = 3;
  public static final TextFileSplitter SPLITTER = TextFileSplitter.CSV;
  // For heatmap generation.
  public static final int H_LEVEL = 18;
  public static final int L_LEVEL = 12;

  public static void main(String[] args) throws Exception {
    Time windowLength = Time.minutes(WIN_LEN);
    // Drop old tables in HBase
    new HBaseCatalogCleaner(ZOOKEEPERS).deleteTable(CATALOG_NAME, TILE_SCHEMA_NAME);
    new HBaseCatalogCleaner(ZOOKEEPERS).deleteTable(CATALOG_NAME, POINTS_SCHEMA_NAME);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.getConfig().setAutoWatermarkInterval(1000L);
    env.setParallelism(PARALLELISM);
    env.disableOperatorChaining();

    // Get heatmap sink function
    Configuration confForTiles = new Configuration();
    confForTiles.setString("geomesa.data.store", "hbase");
    confForTiles.setString("hbase.catalog", "Xiamen");
    confForTiles.setString("geomesa.schema.name", "Heatmap");
    confForTiles.setString("hbase.zookeepers", "localhost:2181");
    confForTiles.setString("geomesa.primary.field.name", "id");
    confForTiles.setString("geomesa.indices.enabled", "id");
    List<Tuple2<String, GeoMesaType>> fieldNamesToTypesForTile = new LinkedList<>();
    fieldNamesToTypesForTile.add(new Tuple2<>("id", GeoMesaType.STRING));
    fieldNamesToTypesForTile.add(new Tuple2<>("tileId", GeoMesaType.LONG));
    fieldNamesToTypesForTile.add(new Tuple2<>("endTime", GeoMesaType.DATE));
    fieldNamesToTypesForTile.add(new Tuple2<>("data", GeoMesaType.POLYGON));
    GeoMesaDataStoreParam tileDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    tileDataStoreParam.initFromConfigOptions(confForTiles);
    GeoMesaStreamTableSchema heatMapSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForTile, confForTiles);
    GeoMesaSinkFunction heatMapSink = new GeoMesaSinkFunction(tileDataStoreParam, heatMapSchema, new AvroStringTileResultToSimpleFeatureConverter(heatMapSchema));

    // Get polygon source function
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
    GeoMesaDataStoreParam polygonDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    polygonDataStoreParam.initFromConfigOptions(confForPolygon);
    GeoMesaStreamTableSchema polygonSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForPolygon, confForPolygon);
    GeoMesaSourceFunction polygonSource = new GeoMesaSourceFunction(polygonDataStoreParam, polygonSchema, new SimpleFeatureToGeometryConverter(polygonSchema));
    SpatialDataStream<Polygon> polygonsds = new SpatialDataStream<Polygon>(env, polygonSource, GeometryType.POLYGON);
    BroadcastSpatialDataStream bsd = new BroadcastSpatialDataStream(polygonsds);

    // Get point sink function
    Configuration confForOutputPoints = new Configuration();
    confForOutputPoints.setString("geomesa.schema.name", POINTS_SCHEMA_NAME);
    confForOutputPoints.setString("geomesa.spatial.fields", "point2:Point");
    confForOutputPoints.setString("hbase.zookeepers", ZOOKEEPERS);
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
    GeoMesaDataStoreParam pointDataStoreParam = GeoMesaDataStoreParamFactory.createGeomesaDataStoreParam("HBase");
    pointDataStoreParam.initFromConfigOptions(confForOutputPoints);
    GeoMesaStreamTableSchema pointSchema = new GeoMesaStreamTableSchema(fieldNamesToTypesForPoints, confForOutputPoints);
    GeoMesaSinkFunction pointSink = new GeoMesaSinkFunction<>(pointDataStoreParam, pointSchema, new PointToSimpleFeatureConverter(pointSchema));
    // 模拟流
    SpatialDataStream<Point> originalDataStream = new SpatialDataStream<Point>(
            env, new CSVStringSourceSimulation(FILEPATH, SPEED_UP, TIMEFIELDINDEX, SPLITTER, false),
            4, 5, TextFileSplitter.CSV, GeometryType.POINT, true,
            Schema.types(Integer.class, Double.class, Integer.class, Long.class, String.class, String.class))
            .assignTimestampsAndWatermarks((WatermarkStrategy.<Point>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                    .withTimestampAssigner((event, timestamp) -> ((Tuple) event.getUserData()).getField(TIMEFIELDINDEX))));
    // 热力图生成
    WindowAssigner assigner = SlidingEventTimeWindows.of(windowLength, Time.minutes(5));
    TileDataStream tileDataStream = new TileDataStream(originalDataStream, new CountAggregator(), assigner, H_LEVEL, L_LEVEL);
    tileDataStream.getTileResultDataStream().addSink(heatMapSink);
    // 围栏Join
    originalDataStream.spatialDimensionJoin(bsd, TopologyType.N_CONTAINS, new AddFenceId(), new TypeHint<Point>() { })
            .addSink(pointSink);

    env.execute();

  }

  private static class CountAggregator implements AggregateFunction<Tuple2<PixelResult<Integer>, Point>, Map<Pixel, Tuple2<Integer, HashSet<String>>>, TileResult<Integer>> {

    @Override
    public Map<Pixel, Tuple2<Integer, HashSet<String>>> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<Pixel, Tuple2<Integer, HashSet<String>>> add(Tuple2<PixelResult<Integer>, Point> inPiexl, Map<Pixel, Tuple2<Integer, HashSet<String>>> pixelIntegerMap) {
      Pixel pixel = inPiexl.f0.getPixel();
      String carNo = ((Tuple) inPiexl.f1.getUserData()).getField(CARNO_FIELD_UDINDEX);
      try {
        if (!pixelIntegerMap.containsKey(pixel)) {
          HashSet<String> carNos = new HashSet<>();
          carNos.add(carNo);
          pixelIntegerMap.put(pixel, new Tuple2<>(1, carNos));
        } else if (!pixelIntegerMap.get(pixel).f1.contains(carNo)) {
          pixelIntegerMap.get(pixel).f1.add(carNo);
          pixelIntegerMap.get(pixel).f0 = pixelIntegerMap.get(pixel).f0 + 1;
        } // 该像素已经出现过，但是车辆尚未在其中出现过。
      } catch (Exception e) {
        e.printStackTrace();
      }
      return pixelIntegerMap;
    }

    @Override
    public TileResult<Integer> getResult(Map<Pixel, Tuple2<Integer, HashSet<String>>> pixelIntegerMap) {
      TileResult<Integer> ret = new TileResult<>();
      ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
      for (Map.Entry<Pixel, Tuple2<Integer, HashSet<String>>> entry : pixelIntegerMap.entrySet()) {
        ret.addPixelResult(new PixelResult<>(entry.getKey(), entry.getValue().f0));
      }
      return ret;
    }

    @Override
    public Map<Pixel, Tuple2<Integer, HashSet<String>>> merge(Map<Pixel, Tuple2<Integer, HashSet<String>>> acc0, Map<Pixel, Tuple2<Integer, HashSet<String>>> acc1) {
      Map<Pixel, Tuple2<Integer, HashSet<String>>> acc2 = new HashMap<>(acc0);
      acc1.forEach((key, value) -> acc2.merge(key, value, (v1, v2) -> new Tuple2<>(v1.f0 + v1.f0, combineSets(v1.f1, v2.f1))));
      return acc2;
    }

    private HashSet<String> combineSets(HashSet<String> v1, HashSet<String> v2) {
      v1.addAll(v2);
      return v1;
    }
  }

  private static class AddFenceId implements JoinFunction<Point, Polygon, Point> {
    @Override
    public Point join(Point first, Polygon second) throws Exception {
      String fid = ((Tuple) second.getUserData()).getField(0);
      Tuple oldUserData = (Tuple) first.getUserData();
      Tuple newUserData = Tuple.newInstance(oldUserData.getArity() + 1);
      for (int i = 0; i < oldUserData.getArity(); i++) {
        newUserData.setField(oldUserData.getField(i), i);
      }
      newUserData.setField(fid, newUserData.getArity() - 1);
      first.setUserData(newUserData);
      return first;
    }
  }
}
