package com.github.tm.glink.examples.aggregate;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.utils.BroadcastFlatMapFunction;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class SpatialMonitoringWithGrid {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    // map function to parse the socket text
    // text format: id,wkt,time
    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();
    FlatMapFunction<String, Tuple2<Boolean, Geometry>> broadcastFlatMapFunction = new BroadcastFlatMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 = new SpatialDataStream<>(env, "localhost", 8888, flatMapFunction);
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(env, "localhost", 9999, broadcastFlatMapFunction);

    // 1. do join
    DataStream<Tuple2<Geometry, Geometry>> joinedStream = spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(1),
            Tuple2::new,
            new TypeHint<Tuple2<Geometry, Geometry>>() {
            });
    // 2. do monitoring
    joinedStream
            // assign timestamps and watermarks
            .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<Tuple2<Geometry, Geometry>>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((event, time) -> {
                      Tuple tuple = (Tuple) event.f0.getUserData();
                      return tuple.getField(1);
                    }))
            // assign grid
//            .map(null)
            // key by grid
            .keyBy(t -> {
              Tuple attr = (Tuple) t.f1.getUserData();
              return (String) attr.getField(0);
            })
            // do window
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            // calculate the aggregate value of each grid in current window
            .apply(new WindowFunction<Tuple2<Geometry, Geometry>, Tuple2<String, Integer>, String, TimeWindow>() {
              @Override
              public void apply(String key,
                                TimeWindow timeWindow,
                                Iterable<Tuple2<Geometry, Geometry>> iterable,
                                Collector<Tuple2<String, Integer>> collector) throws Exception {
                AtomicInteger count = new AtomicInteger();
                iterable.forEach(t -> count.incrementAndGet());
                if (count.get() > 3) {
                  collector.collect(new Tuple2<>(key, count.get()));
                }
              }
            })
            // key by monitoring area
//            .keyBy()
            // do clustering of each area
            .print();

    env.execute();
  }
}
