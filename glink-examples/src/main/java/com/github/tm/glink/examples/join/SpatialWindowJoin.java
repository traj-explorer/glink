package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

/**
 * A simple example of how to use glink to perform spatial window join.
 *
 * @author Yu Liebing
 * */
public class SpatialWindowJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();

    SpatialDataStream<Geometry> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 9000, flatMapFunction)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Geometry>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> {
                                      System.out.println(event.getUserData());
                                      return ((Tuple3<String, Long, String>) event.getUserData()).f1;
                                    }));
    SpatialDataStream<Geometry> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, "localhost", 9001, flatMapFunction)
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                            .<Geometry>forBoundedOutOfOrderness(Duration.ZERO)
                            .withTimestampAssigner(
                                    (event, time) -> ((Tuple3<String, Long, String>) event.getUserData()).f1));

    DataStream<String> dataStream = pointSpatialDataStream1.spatialWindowJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(1),
            TumblingEventTimeWindows.of(Time.seconds(5)),
            (point, point2) -> point + ", " + point2,
            new TypeHint<String>() { });
    dataStream.print();
    pointSpatialDataStream1.print();

    env.execute();
  }
}
