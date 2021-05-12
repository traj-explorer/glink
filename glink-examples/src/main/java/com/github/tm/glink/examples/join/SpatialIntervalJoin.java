package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.distance.GeographicalDistanceCalculator;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

/**
 * A simple example of how to use glink to perform spatial window join.
 *
 * @author Yu Liebing
 * */
public class SpatialIntervalJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    SpatialDataStream.distanceCalculator = new GeographicalDistanceCalculator();
    SpatialDataStream<Geometry> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 8888, new SpatialFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    SpatialDataStream<Geometry> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, "localhost", 9999, new SpatialFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);

    DataStream<String> dataStream = pointSpatialDataStream1.spatialIntervalJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(10),
            Time.seconds(-5),
            Time.seconds(5),
            (point, point2) -> point + ", " + point2,
            new TypeHint<String>() { });
    dataStream.print();

    env.execute();
  }
}
