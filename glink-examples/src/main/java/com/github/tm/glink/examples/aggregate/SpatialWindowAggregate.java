package com.github.tm.glink.examples.aggregate;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.utils.BroadcastFlatMapFunction;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

public class SpatialWindowAggregate {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // map function to parse the socket text
    // text format: id,wkt,time(HH:mm:ss)
    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();
    FlatMapFunction<String, Tuple2<Boolean, Geometry>> broadcastFlatMapFunction = new BroadcastFlatMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 = new SpatialDataStream<>(
            env, "localhost", 8888, flatMapFunction);
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(
            env, "localhost", 9999, broadcastFlatMapFunction);

    // 1. do join
    DataStream<Tuple2<Geometry, Geometry>> joinedStream = spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(1),
            Tuple2::new,
            new TypeHint<Tuple2<Geometry, Geometry>>() { });

    joinedStream = joinedStream.assignTimestampsAndWatermarks(
            WatermarkStrategy
                    .<Tuple2<Geometry, Geometry>>forMonotonousTimestamps()
                    .withTimestampAssigner((event, time) -> {
                      Tuple2<String, Long> attr = (Tuple2<String, Long>) event.f0.getUserData();
                      return attr.f1;
                    }));

    // 2. do aggregate
    joinedStream
            .keyBy(t -> {
              Tuple2<String, Long> attr = (Tuple2<String, Long>) t.f1.getUserData();
              return attr.f0;
            })
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .aggregate(new AggregateFunction<Tuple2<Geometry, Geometry>, Tuple2<String, Long>, Tuple2<String, Long>>() {
              @Override
              public Tuple2<String, Long> createAccumulator() {
                return new Tuple2<>();
              }

              @Override
              public Tuple2<String, Long> add(Tuple2<Geometry, Geometry> in, Tuple2<String, Long> acc) {
                return null;
              }

              @Override
              public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
                return null;
              }

              @Override
              public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
                return null;
              }
            })
            .print();

    env.execute();
  }
}
