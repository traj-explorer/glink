package com.github.tm.glink.core.datastream;

import com.github.tm.glink.core.distance.DistanceCalculator;
import com.github.tm.glink.core.distance.GeographicalDistanceCalculator;
import com.github.tm.glink.core.distance.GeometricDistanceCalculator;
import com.github.tm.glink.core.enums.GeometryType;
import com.github.tm.glink.core.enums.TextFileSplitter;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.format.TextFormatMap;
import com.github.tm.glink.core.geom.Circle;
import com.github.tm.glink.core.index.*;
import com.github.tm.glink.core.operator.join.BroadcastJoinFunction;
import com.github.tm.glink.core.operator.join.BroadcastKNNJoinFunction;
import com.github.tm.glink.core.operator.join.JoinWithTopologyType;
import com.github.tm.glink.core.operator.aggregate.PVAggregateFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

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
   * <p>
   * In the {@link SpatialDataStream}, generic type {@link T} is a subclass of {@link Geometry}.
   * If it has non-spatial attributes, it can be obtained through {@link Geometry#getUserData}.
   * It is a flink {@link org.apache.flink.api.java.tuple.Tuple} type.
   * */
  private DataStream<T> spatialDataStream;

  private DataStream<Tuple2<Long, T>> gridDataStream;

  private int timestampField = -1;

  /**
   * Grid index, default is {@link GeographicalGridIndex}
   * */
  public static GridIndex gridIndex = new GeographicalGridIndex(15);

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
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction);
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<T> sourceFunction,
                           GeometryType geometryType) {
    this.env = env;
    spatialDataStream = env
            .addSource(sourceFunction)
            .returns(geometryType.getTypeInformation());
  }

  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final SourceFunction<String> sourceFunction,
                           final int geometryStartOffset,
                           final int geometryEndOffset,
                           final TextFileSplitter splitter,
                           final GeometryType geometryType,
                           final boolean carryAttributes,
                           final Class<?>[] types) {
    this.env = env;
    this.geometryType = geometryType;
    TextFormatMap<T> textFormatMap = new TextFormatMap<>(
            geometryStartOffset, geometryEndOffset, splitter, geometryType, carryAttributes, types);
    spatialDataStream = env
            .addSource(sourceFunction)
            .rebalance()
            .flatMap(textFormatMap)
            .returns(geometryType.getTypeInformation());
  }

  /**
   * Init a {@link SpatialDataStream} from a text file.
   * */
  public SpatialDataStream(final StreamExecutionEnvironment env,
                           final String path,
                           final FlatMapFunction<String, T> flatMapFunction) {
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
    timestampField = field;
    spatialDataStream = spatialDataStream
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<T>forBoundedOutOfOrderness(maxOutOfOrderness)
                    .withTimestampAssigner((event, time) -> {
                      Tuple t = (Tuple) event.getUserData();
                      return t.getField(field);
                    }));
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
   * Spatial window join of two {@link SpatialDataStream}
   * */
  public <T2 extends Geometry, W extends Window, OUT> DataStream<OUT> spatialWindowJoin(
          SpatialDataStream<T2> joinStream,
          TopologyType joinType,
          WindowAssigner<? super CoGroupedStreams.TaggedUnion<Tuple2<Long, T>, Tuple2<Long, T2>>, W> assigner,
          JoinFunction<T, T2, OUT> joinFunction,
          TypeHint<OUT> returnType) {
    this.assignGridsInternal(gridIndex);
    joinStream.assignGridsAndDistribute(gridIndex, joinType);

    DataStream<Tuple2<Long, T>> indexedStream1 = this.gridDataStream;
    DataStream<Tuple2<Long, T2>> indexedStream2 = joinStream.gridDataStream;

    return indexedStream1.coGroup(indexedStream2)
            .where(t -> t.f0).equalTo(t -> t.f0)
            .window(assigner)
            .apply(new CoGroupFunction<Tuple2<Long, T>, Tuple2<Long, T2>, Object>() {
              @Override
              public void coGroup(Iterable<Tuple2<Long, T>> stream1,
                                  Iterable<Tuple2<Long, T2>> stream2,
                                  Collector<Object> collector) throws Exception {
                TreeIndex<T2> treeIndex = new STRTreeIndex<>();
                stream2.forEach(t2 -> treeIndex.insert(t2.f1));
                if (joinType == TopologyType.WITHIN_DISTANCE) {
                  for (Tuple2<Long, T> t1 : stream1) {
                    List<T2> result = treeIndex.query(t1.f1, joinType.getDistance(), distanceCalculator);
                    for (T2 r : result) {
                      collector.collect(joinFunction.join(t1.f1, r));
                    }
                  }
                } else {
                  for (Tuple2<Long, T> t1 :stream1) {
                    List<T2> result = treeIndex.query(t1.f1);
                    for (T2 r : result) {
                      JoinWithTopologyType
                              .join(t1.f1, r, joinType, joinFunction, distanceCalculator)
                              .ifPresent(collector::collect);
                    }
                  }
                }
              }
            })
            .map(new MapFunction<Object, OUT>() {
              @Override
              public OUT map(Object out) throws Exception {
                return (OUT) out;
              }
            })
            .returns(returnType);
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

  public <T2 extends Geometry, OUT, KEY> DataStream<Tuple2<OUT, List<T>>> spatialWindowKNN(
          BroadcastSpatialDataStream<T2> knnQueryStream,
          MapFunction<T2, OUT> mapFunction,
          int k,
          double distance,
          WindowAssigner<Object, TimeWindow> windowAssigner,
          KeySelector<OUT, KEY> keySelector,
          TypeHint<Tuple4<Long, Double, T, OUT>> joinReturnType,
          TypeHint<Tuple2<OUT, List<T>>> returnType) {
    this.assignGridsInternal(gridIndex);

    DataStream<Tuple2<Long, T>> dataStream1 = gridDataStream;
    DataStream<Tuple2<Boolean, T2>> dataStream2 = knnQueryStream.getDataStream();
    final MapStateDescriptor<Integer, TreeIndex<Circle>> broadcastDesc = new MapStateDescriptor<>(
            "broadcast-state-for-knn",
            TypeInformation.of(Integer.class),
            TypeInformation.of(new TypeHint<TreeIndex<Circle>>() { }));
    BroadcastStream<Tuple2<Boolean, T2>> broadcastStream = dataStream2.broadcast(broadcastDesc);
    BroadcastKNNJoinFunction<T, T2, OUT> broadcastKNNJoinFunction =
            new BroadcastKNNJoinFunction<>(distance, mapFunction, distanceCalculator);
    broadcastKNNJoinFunction.setBroadcastDesc(broadcastDesc);
    return dataStream1
            .connect(broadcastStream)
            .process(broadcastKNNJoinFunction)
            .returns(joinReturnType)
            .assignTimestampsAndWatermarks(WatermarkStrategy
            .<Tuple4<Long, Double, T, OUT>>forBoundedOutOfOrderness(Duration.ZERO)
            .withTimestampAssigner((event, time) -> {
              Tuple t = (Tuple) event.f2.getUserData();
              return t.getField(1);
            }))
            .keyBy(t -> t.f0)
            .window(windowAssigner)
            .apply(new WindowFunction<Tuple4<Long, Double, T, OUT>, Tuple2<OUT, List<Tuple2<Double, T>>>, Long, TimeWindow>() {
              @Override
              public void apply(Long key,
                                TimeWindow timeWindow,
                                Iterable<Tuple4<Long, Double, T, OUT>> iterable,
                                Collector<Tuple2<OUT, List<Tuple2<Double, T>>>> collector) throws Exception {
                Map<KEY, PriorityQueue<Tuple2<Double, T>>> map = new HashMap<>();
                Map<KEY, OUT> keyMap = new HashMap<>();
                for (Tuple4<Long, Double, T, OUT> t4 : iterable) {
                  PriorityQueue<Tuple2<Double, T>> pq = map.computeIfAbsent(keySelector.getKey(t4.f3),
                          r -> new PriorityQueue<>(Comparator.comparingDouble(t -> -t.f0)));
                  keyMap.put(keySelector.getKey(t4.f3), t4.f3);
                  pq.offer(new Tuple2<>(t4.f1, t4.f2));
                  if (pq.size() > k) pq.poll();
                }
//                map.forEach((k, v) -> collector.collect(new Tuple2<>(keyMap.get(k), v)));
                for (Map.Entry<KEY, PriorityQueue<Tuple2<Double, T>>> e : map.entrySet()) {
                  Tuple2<OUT, PriorityQueue<Tuple2<Double, T>>> t = new Tuple2<>(keyMap.get(e.getKey()), e.getValue());
                  List<Tuple2<Double, T>> list = new ArrayList<>(e.getValue());
                  collector.collect(new Tuple2<>(t.f0, list));
                }
              }
            })
            .keyBy(t -> keySelector.getKey(t.f0))
            .window(windowAssigner)
            .apply(new WindowFunction<Tuple2<OUT, List<Tuple2<Double, T>>>, Tuple2<OUT, List<T>>, KEY, TimeWindow>() {
              @Override
              public void apply(KEY key,
                                TimeWindow timeWindow,
                                Iterable<Tuple2<OUT, List<Tuple2<Double, T>>>> iterable,
                                Collector<Tuple2<OUT, List<T>>> collector) throws Exception {
                PriorityQueue<Tuple2<Double, T>> pq = new PriorityQueue<>(Comparator.comparingDouble(t -> -t.f0));
                OUT out = null;
                for (Tuple2<OUT, List<Tuple2<Double, T>>> t2 : iterable) {
                  if (out == null) {
                    out = t2.f0;
                  }
                  for (Tuple2<Double, T> t : t2.f1) {
                    pq.offer(t);
                    if (pq.size() > k) pq.poll();
                  }
                }
                List<T> list = pq.stream().map(t -> t.f1).collect(Collectors.toList());
                collector.collect(new Tuple2<>(out, list));
              }
            })
            .returns(returnType);
  }

  public static <IN, KEY, W extends Window, OUT> DataStream<Tuple2<OUT, Long>> spatialPVAggregate(
          DataStream<IN> joinedStream,
          KeySelector<IN, KEY> keySelector,
          WindowAssigner<? super IN, W> assigner,
          MapFunction<IN, Object> outMapper,
          TypeHint<Tuple2<OUT, Long>> returnType) {
    return joinedStream
            .keyBy(keySelector)
            .window(assigner)
            .aggregate(new PVAggregateFunction<>(outMapper))
            .map(t -> new Tuple2<>((OUT) t.f0, t.f1))
            .returns(returnType);
  }

  public static <IN, KEY, W extends Window, OUT> DataStream<OUT> spatialWindowApply(
          DataStream<IN> joinedStream,
          KeySelector<IN, KEY> keySelector,
          WindowAssigner<? super IN, W> assigner,
          WindowFunction<IN, OUT, KEY, W> windowFunction) {
    return joinedStream
            .keyBy(keySelector)
            .window(assigner)
            .apply(windowFunction);
  }

  public static <IN, KEY, W extends Window, OUT, G extends Geometry, OUT1> DataStream<OUT1> spatialGridWindowApply(
          DataStream<IN> joinedStream,
          MapFunction<IN, G> geomExtractor,
          WindowAssigner<Tuple2<Long, ? super IN>, W> keyedAssigner,
          WindowFunction<Tuple2<Long, IN>, OUT, Long, W> keyedWindowFunction,
          KeySelector<OUT, KEY> keySelector,
          WindowAssigner<? super OUT, W> assigner,
          WindowFunction<OUT, OUT1, KEY, W> windowFunction) {
    return joinedStream
            .flatMap(new FlatMapFunction<IN, Tuple2<Long, IN>>() {
              @Override
              public void flatMap(IN in, Collector<Tuple2<Long, IN>> collector) throws Exception {
                List<Long> indices = gridIndex.getIndex(geomExtractor.map(in));
                indices.forEach(index -> collector.collect(new Tuple2<>(index, in)));
              }
            })
            .keyBy(t -> t.f0)
            .window(keyedAssigner)
            .apply(keyedWindowFunction)
            .keyBy(keySelector)
            .window(assigner)
            .apply(windowFunction);
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

  private void assignGridsInternal(GridIndex gridIndex) {
    gridDataStream = spatialDataStream
            .flatMap(new FlatMapFunction<T, Tuple2<Long, T>>() {
              @Override
              public void flatMap(T geom, Collector<Tuple2<Long, T>> collector) throws Exception {
                List<Long> indices = gridIndex.getIndex(geom);
                indices.forEach(index -> collector.collect(new Tuple2<>(index, geom)));
              }
            });
  }

  private void assignGridsAndDistribute(GridIndex gridIndex, TopologyType type) {
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
                indices.forEach(index -> collector.collect(new Tuple2<>(index, geom)));
              }
            });
  }

}
