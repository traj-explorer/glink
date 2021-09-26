package com.github.tm.glink.examples.aggregate;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.process.SpatialDimensionJoin;
import com.github.tm.glink.examples.utils.BroadcastFlatMapFunction;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 * */
public class SpatialGridWindowAggregate {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // map function to parse the socket text
    // text format: id,wkt,time
    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();
    FlatMapFunction<String, Tuple2<Boolean, Geometry>> broadcastFlatMapFunction = new BroadcastFlatMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 = new SpatialDataStream<>(
            env, "localhost", 8888, flatMapFunction);
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(
            env, "localhost", 9999, broadcastFlatMapFunction);

    // 1. do join
    DataStream<Tuple2<Geometry, Geometry>> joinedStream = SpatialDimensionJoin.join(
            spatialDataStream1,
            spatialDataStream2,
            TopologyType.N_CONTAINS,
            Tuple2::new,
            new TypeHint<Tuple2<Geometry, Geometry>>() { });

    joinedStream = joinedStream.assignTimestampsAndWatermarks(
            WatermarkStrategy
                    .<Tuple2<Geometry, Geometry>>forMonotonousTimestamps()
                    .withTimestampAssigner((event, time) -> {
                      Tuple2<String, Long> attr = (Tuple2<String, Long>) event.f0.getUserData();
                      return attr.f1;
                    }));

    // 2. do monitoring
    joinedStream
            .flatMap(new GridAssigner())  // assign grid
            .keyBy(t -> t.f0)             // key by grid
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new KeyedWindow())
            .keyBy(t -> t.f0)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(new AllWindow())
            .print();

    env.execute();
  }

  public static class GridAssigner implements
          FlatMapFunction<Tuple2<Geometry, Geometry>, Tuple2<Long, Tuple2<Geometry, Geometry>>> {
    @Override
    public void flatMap(Tuple2<Geometry, Geometry> item, Collector<Tuple2<Long, Tuple2<Geometry, Geometry>>> collector) throws Exception {
      List<Long> indices = SpatialDataStream.gridIndex.getIndex(item.f0);
      indices.forEach(index -> collector.collect(new Tuple2<>(index, item)));
    }
  }

  public static class KeyedWindow implements
          WindowFunction<Tuple2<Long, Tuple2<Geometry, Geometry>>, Tuple2<String, Integer>, Long, TimeWindow> {
    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple2<Long, Tuple2<Geometry, Geometry>>> iterable,
                      Collector<Tuple2<String, Integer>> collector) throws Exception {
      int count = 0;
      String pkey = null;
      for (Tuple2<Long, Tuple2<Geometry, Geometry>> t : iterable) {
        ++count;
        if (pkey == null) {
          Tuple2<String, Long> attr = (Tuple2<String, Long>) t.f1.f1.getUserData();
          pkey = attr.f0;
        }
      }
      collector.collect(new Tuple2<>(pkey, count));
    }
  }

  public static class AllWindow implements
          WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
    @Override
    public void apply(String key, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
      int count = 0;
      for (Tuple2<String, Integer> t : iterable) {
        count += t.f1;
      }
      collector.collect(new Tuple2<>(key, count));
    }
  }
}
