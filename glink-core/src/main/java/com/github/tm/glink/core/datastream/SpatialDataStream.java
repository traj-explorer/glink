package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.distance.DistanceCalculator;
import com.github.tm.glink.core.distance.GeographicalDistanceCalculator;
import com.github.tm.glink.core.distance.GeometricDistanceCalculator;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.format.TextFormatMap;
import com.github.tm.glink.core.index.*;
import com.github.tm.glink.core.operator.join.BroadcastJoinFunction;
import com.github.tm.glink.core.operator.join.JoinWithTopologyType;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.core.util.GeoUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
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

import java.time.Duration;
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
   * It is a flink {@link org.apache.flink.api.java.tuple.Tuple} type.
   * */
  private DataStream<T> spatialDataStream;

  private DataStream<Tuple2<Long, T>> gridDataStream;

  /**
   * Grid index, default is {@link GeographicalGridIndex}
   * */
  private GridIndex gridIndex = new GeographicalGridIndex(15);

  /**
   * Distance calculator, default is {@link GeometricDistanceCalculator}.
   * */
  public static DistanceCalculator distanceCalculator = new GeographicalDistanceCalculator();

  protected SpatialDataStream() { }

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

  /**
   * Init a {@link SpatialDataStream} from a text file.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String path,
                           final FlatMapFunction<String, T> flatMapFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    spatialDataStream = env
            .readTextFile(path)
            .flatMap(flatMapFunction);
  }

  /**
   * Init a {@link SpatialDataStream} from socket.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String host,
                           final int port,
                           final String delimiter,
                           final FlatMapFunction<String, T> flatMapFunction) {
    GlinkSerializerRegister.registerSerializer(env);
    this.env = env;
    spatialDataStream = env
            .socketTextStream(host, port, delimiter)
            .flatMap(flatMapFunction);
  }

  /**
   * Init a {@link SpatialDataStream} from socket.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String host,
                           final int port,
                           final FlatMapFunction<String, T> flatMapFunction) {
    this(env, host, port, "\n", flatMapFunction);
  }

  public SpatialDataStream<T> assignTimestampsAndWatermarks(WatermarkStrategy<T> watermarkStrategy) {
    spatialDataStream = spatialDataStream.assignTimestampsAndWatermarks(watermarkStrategy);
    return this;
  }

  public SpatialDataStream<T> assignBoundedOutOfOrdernessWatermarks(Duration maxOutOfOrderness, int field) {
    spatialDataStream = spatialDataStream
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                    .withTimestampAssigner((event, time) -> {
                      Tuple t = (Tuple) event.getUserData();
                      return t.getField(field);
                    }));
    return this;
  }

  public SpatialDataStream<T> assignGrids(GridIndex gridIndex) {
    this.gridIndex = gridIndex;
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
   */
  public SpatialDataStream<T> crsTransform(String sourceEpsgCRSCode, String targetEpsgCRSCode, boolean lenient) {
    try {
      CoordinateReferenceSystem sourceCRS = CRS.decode(sourceEpsgCRSCode);
      CoordinateReferenceSystem targetCRS = CRS.decode(targetEpsgCRSCode);
      final MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS, lenient);
      spatialDataStream = spatialDataStream.map(r -> (T) JTS.transform(r, transform));
      return this;
    } catch (FactoryException e) {
      e.printStackTrace();
    }
    return null;
  }

  /**
   * Spatial dimension join with external storage systems, such as geomesa.
   * */
  public <T2 extends Geometry, OUT> DataStream<OUT> spatialDimensionJoin() {
    return null;
  }

  /**
   * Spatial dimension join with a broadcast stream.
   * @param joinStream another {@link SpatialDataStream} to join with, will be treated as broadcast stream
   * @param returnType the return type of join
   * */
  public <T2 extends Geometry, OUT> DataStream<OUT> spatialDimensionJoin(
          BroadcastSpatialDataStream<T2> joinStream,
          TopologyType joinType,
          JoinFunction<T, T2, OUT> joinFunction,
          TypeHint<OUT> returnType) {
    DataStream<T> dataStream1 = spatialDataStream;
    DataStream<Tuple2<Boolean, T2>> dataStream2 = joinStream.getDataStream();
    final MapStateDescriptor<Integer, TreeIndex<T2>> broadcastDesc = new MapStateDescriptor<>(
            "broadcast-state-for-dim-join",
            TypeInformation.of(Integer.class),
            TypeInformation.of(new TypeHint<TreeIndex<T2>>() { }));
    BroadcastStream<Tuple2<Boolean, T2>> broadcastStream = dataStream2.broadcast(broadcastDesc);
    BroadcastJoinFunction<T, T2, OUT> broadcastJoinFunction = new BroadcastJoinFunction<>(joinType, joinFunction);
    broadcastJoinFunction.setBroadcastDesc(broadcastDesc);
    return dataStream1
            .connect(broadcastStream)
            .process(broadcastJoinFunction)
            .returns(returnType);
  }

  public <T2 extends Geometry, W extends Window> DataStream<Object> spatialWindowJoin(
          SpatialDataStream<T2> joinStream,
          TopologyType joinType,
          WindowAssigner<? super CoGroupedStreams.TaggedUnion<Tuple2<T, Long>, Tuple2<T2, Long>>, W> assigner,
          JoinFunction<T, T2, Object> joinFunction,
          double... distance) {
    // create grid index
    final GridIndex gridIndex = new GeographicalGridIndex(17);
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
        if (joinType == TopologyType.WITHIN_DISTANCE) {
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

  /**
   * Spatial interval join for two streams. Currently only support:
   * point stream with point stream,
   * point stream with any kind of geometry stream,
   * any kind of geometry stream with point stream.
   * */
  public <T2 extends Geometry, OUT> DataStream<OUT> spatialIntervalJoin(
          SpatialDataStream<T2> joinStream,
          TopologyType joinType,
          Time lowerBound,
          Time upperBound,
          JoinFunction<T, T2, OUT> joinFunction,
          TypeHint<OUT> returnType) {
    this.assignGridsInternal(gridIndex);
    joinStream.assignGridsAndDistribute(gridIndex, joinType);

    DataStream<Tuple2<Long, T>> gridDataStream1 = this.gridDataStream;
    DataStream<Tuple2<Long, T2>> gridDataStream2 = joinStream.gridDataStream;
    return gridDataStream1
            .keyBy(t -> t.f0)
            .intervalJoin(gridDataStream2.keyBy(t -> t.f0))
            .between(lowerBound, upperBound)
            .process(new ProcessJoinFunction<Tuple2<Long, T>, Tuple2<Long, T2>, OUT>() {

              @Override
              public void processElement(Tuple2<Long, T> t1,
                                         Tuple2<Long, T2> t2,
                                         Context context, Collector<OUT> collector) throws Exception {
                JoinWithTopologyType
                        .join(t1.f1, t2.f1, joinType, joinFunction, distanceCalculator)
                        .ifPresent(collector::collect);
              }
            })
            .returns(returnType);
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

  private SpatialDataStream<T> assignGridsInternal(GridIndex gridIndex) {
    gridDataStream = spatialDataStream
            .flatMap(new FlatMapFunction<T, Tuple2<Long, T>>() {
              @Override
              public void flatMap(T geom, Collector<Tuple2<Long, T>> collector) throws Exception {
                List<Long> indices = gridIndex.getIndex(geom);
                indices.forEach(index -> collector.collect(new Tuple2<>(index, geom)));
              }
            });
    return this;
  }

  private SpatialDataStream<T> assignGridsAndDistribute(GridIndex gridIndex, TopologyType type) {
    gridDataStream = spatialDataStream
            .flatMap(new FlatMapFunction<T, Tuple2<Long, T>>() {
              @Override
              public void flatMap(T geom, Collector<Tuple2<Long, T>> collector) throws Exception {
                List<Long> indices;
                if (type == TopologyType.WITHIN_DISTANCE) {
                  Envelope box = distanceCalculator.calcBoxByDist(geom, type.getDistance());
                  indices = gridIndex.getRangeIndex(box.getMinY(), box.getMinX(), box.getMaxY(), box.getMaxX());
                } else {
                  indices = gridIndex.getIndex(geom);
                }
                System.out.println("Distribute grids: " + indices.size());
                indices.forEach(index -> collector.collect(new Tuple2<>(index, geom)));
              }
            });
    return this;
  }

}
