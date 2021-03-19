package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.SpatialJoinType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.format.TextFormatMap;
import com.github.tm.glink.core.index.GridIndex;
import com.github.tm.glink.core.index.STRTreeIndex;
import com.github.tm.glink.core.index.TreeIndex;
import com.github.tm.glink.core.index.UGridIndex;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.core.util.GeoUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class SpatialDataStream<T extends Geometry> {

  /**
   * The stream execution environment
   * */
  protected StreamExecutionEnvironment env;

  protected GeometryType geometryType;

  /**
   * The origin flink {@link DataStream}, be private so the subclasses will not see
   * and need to maintain their own {@link DataStream}.
   *
   * In the {@link SpatialDataStream}, generic type {@link T} is a subclass of {@link Geometry}.
   * If it has non-spatial attributes, it can be obtained through {@link Geometry#getUserData}.
   * It is a flink tuple type.
   * */
  private DataStream<T> spatialDataStream;

  protected SpatialDataStream() { }

  /**
   * Init a {@link SpatialDataStream} from an origin flink {@link DataStream}.
   * */
  public SpatialDataStream(final DataStream<T> dataStream) {
    this.spatialDataStream = dataStream;
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final int geometryEndOffset,
                           final TextFileSplitter splitter,
                           final GeometryType geometryType,
                           final boolean carryAttributes,
                           final Class<?>[] types,
                           final String... elements) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    this.geometryType = geometryType;
    TextFormatMap<T> textFormatMap = new TextFormatMap<>(
            geometryStartOffset, geometryEndOffset, splitter, geometryType, carryAttributes, types);
    spatialDataStream = env
            .fromElements(elements)
            .flatMap(textFormatMap).returns(geometryType.getTypeInformation());
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final TextFileSplitter splitter,
                           final Class<?>[] types,
                           final String... elements) {
    this(env, geometryStartOffset, geometryStartOffset + 1, splitter, GeometryType.POINT, true, types, elements);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final int geometryEndOffset,
                           final TextFileSplitter splitter,
                           final GeometryType geometryType,
                           final boolean carryAttributes,
                           final Class<?>[] types,
                           final String csvPath) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    this.geometryType = geometryType;
    TextFormatMap<T> textFormatMap = new TextFormatMap<>(
            geometryStartOffset, geometryEndOffset, splitter, geometryType, carryAttributes, types);
    spatialDataStream = env
            .readTextFile(csvPath)
            .flatMap(textFormatMap).returns(geometryType.getTypeInformation());
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final int geometryStartOffset,
                           final TextFileSplitter splitter,
                           final Class<?>[] types,
                           final String csvPath) {
    this(env, geometryStartOffset, geometryStartOffset + 1, splitter, GeometryType.POINT, true, types, csvPath);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<T> sourceFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String host,
                           final int port,
                           MapFunction<String, T> mapFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    spatialDataStream = env
            .socketTextStream(host, port)
            .map(mapFunction);
  }

  public SpatialDataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) {
    spatialDataStream = spatialDataStream.assignTimestampsAndWatermarks(watermarkStrategy);
    return this;
  }

  /**
   * CRS transform.
   *
   * @param sourceEpsgCRSCode the source epsg CRS code
   * @param targetEpsgCRSCode the target epsg CRS code
   * @param lenient consider the difference of the geodetic datum between the two coordinate systems,
   *                if {@code true}, never throw an exception "Bursa-Wolf Parameters Required", but not
   *                recommended for careful analysis work
   *
   * @return true, if successful
   */
  @SuppressWarnings("checkstyle:MethodName")
  public boolean CRSTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient) {
    try {
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
      spatialDataStream = spatialDataStream.map(r -> (T) JTS.transform(r, transform));
      return true;
    } catch (FactoryException e) {
      e.printStackTrace();
      return false;
    }
  }

  public <T2 extends Geometry> SpatialDataStream<T> spatialDimensionJoin(
          SpatialDataStream<T2> joinStream,
          SpatialJoinType joinType,
          JoinFunction<T, T2, Object> joinFunction,
          double distance) {
    DataStream<T> dataStream1 = spatialDataStream;
    DataStream<T2> dataStream2 = joinStream.spatialDataStream;
    final MapStateDescriptor<Integer, TreeIndex<T2>> broadcastDesc = new MapStateDescriptor<>(
            "broadcast-state-for-dim-join",
            TypeInformation.of(Integer.class),
            TypeInformation.of(new TypeHint<TreeIndex<T2>>() { }));
    BroadcastStream<T2> broadcastStream = dataStream2.broadcast(broadcastDesc);
    dataStream1.connect(broadcastStream).process(new BroadcastProcessFunction<T, T2, Tuple2<T, String>>() {

      @Override
      public void processElement(T t, ReadOnlyContext readOnlyContext, Collector<Tuple2<T, String>> collector) throws Exception {
        System.out.println(t);
      }

      @Override
      public void processBroadcastElement(T2 t2, Context context, Collector<Tuple2<T, String>> collector) throws Exception {
        BroadcastState<Integer, TreeIndex<T2>> state = context.getBroadcastState(broadcastDesc);
        if (!state.contains(0)) {
          state.put(0, new STRTreeIndex<>(2));
        }
        System.out.println(t2);
      }
    });
    return this;
  }

  public <T2 extends Geometry, W extends Window> DataStream<Object> spatialWindowJoin(
          SpatialDataStream<T2> joinStream,
          SpatialJoinType joinType,
          WindowAssigner<? super CoGroupedStreams.TaggedUnion<Tuple2<T, Long>, Tuple2<T2, Long>>, W> assigner,
          JoinFunction<T, T2, Object> joinFunction,
          double... distance) {
    // create grid index
    final GridIndex gridIndex = new UGridIndex(17);
    DataStream<T> dataStream1 = spatialDataStream;
    DataStream<T2> dataStream2 = joinStream.spatialDataStream;

    DataStream<Tuple2<T, Long>> indexedStream1 = dataStream1.flatMap(new FlatMapFunction<T, Tuple2<T, Long>>() {
      @Override
      public void flatMap(T geom, Collector<Tuple2<T, Long>> collector) throws Exception {
        List<Long> indices = gridIndex.getIndex(geom);
        for (long index : indices) {
          collector.collect(new Tuple2<>(geom, index));
        }
      }
    });

    DataStream<Tuple2<T2, Long>> indexedStream2 = dataStream2.flatMap(new FlatMapFunction<T2, Tuple2<T2, Long>>() {
      @Override
      public void flatMap(T2 geom, Collector<Tuple2<T2, Long>> collector) throws Exception {
        if (joinType == SpatialJoinType.DISTANCE) {
          Point p = geom.getCentroid();
          Envelope box = GeoUtils.calcBoxByDist(p, distance[0]);
          List<Long> indices = gridIndex.getRangeIndex(box.getMinY(), box.getMinX(), box.getMaxY(), box.getMaxX());
          for (long index : indices) {
            collector.collect(new Tuple2<>(geom, index));
          }
        }
      }
    });

    return indexedStream1.coGroup(indexedStream2)
            .where(t -> t.f1).equalTo(t -> t.f1)
            .window(assigner)
            .apply(new CoGroupFunction<Tuple2<T, Long>, Tuple2<T2, Long>, Object>() {
              @Override
              public void coGroup(Iterable<Tuple2<T, Long>> stream1,
                                  Iterable<Tuple2<T2, Long>> stream2,
                                  Collector<Object> collector) throws Exception {
                TreeIndex<T2> treeIndex = new STRTreeIndex<>(2);
                for (Tuple2<T2, Long> t2 : stream2) {
                  treeIndex.insert(t2.f0);
                }
                for (Tuple2<T, Long> t1 : stream1) {
                  List<T2> result = treeIndex.query(t1.f0, distance[0]);
                  for (T2 r : result) {
                    collector.collect(joinFunction.join(t1.f0, r));
                  }
                }
              }
            });
  }

  public DataStream<Object> spatialMonitoring(
          SpatialDataStream<T> monitoringStream) {
    return null;
  }

  public DataStream<Object> spatialHeatmap() {
    return null;
  }

  public DataStream<T> getDataStream() {
    return spatialDataStream;
  }

  public GeometryType getGeometryType() {
    return geometryType;
  }

  public void print() {
    spatialDataStream
            .map(r -> r + " " + r.getUserData())
            .print();
  }

}
