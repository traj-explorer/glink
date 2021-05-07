package com.github.tm.glink.examples.knn;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.serialize.GlinkSerializerRegister;
import com.github.tm.glink.examples.utils.BroadcastFlatMapFunction;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;
import java.util.List;
import java.util.PriorityQueue;

public class SpatialKNN {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    GlinkSerializerRegister.registerSerializer(env);
    env.setParallelism(1);

    // map function to parse the socket text
    // text format: id,wkt,time
    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();
    FlatMapFunction<String, Tuple2<Boolean, Geometry>> broadcastFlatMapFunction = new BroadcastFlatMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 8888, flatMapFunction)
            .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 =
            new BroadcastSpatialDataStream<>(env, "localhost", 9999, broadcastFlatMapFunction);

    spatialDataStream1.spatialWindowKNN(
            spatialDataStream2,
            new MapFunction<Geometry, Geometry>() {
              @Override
              public Geometry map(Geometry geometry) throws Exception {
                return geometry;
              }
            },
            2,
            3,
            TumblingEventTimeWindows.of(Time.seconds(5)),
            new KeySelector<Geometry, String>() {
              @Override
              public String getKey(Geometry geometry) throws Exception {
                Tuple2<String, Long> attr = (Tuple2<String, Long>) geometry.getUserData();
                return attr.f0;
              }
            },
            new TypeHint<Tuple4<Long, Double, Geometry, Geometry>>() { },
            new TypeHint<Tuple2<Geometry, List<Geometry>>>() { })
            .print();

    env.execute();
  }
}
