package com.github.tm.glink.examples.demo.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Point;

import java.time.Duration;

public class SpatialIntervalJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    ParameterTool parameterTool = ParameterTool.fromArgs(args);
    String streamPath1 = parameterTool.get("path1");
    String streamPath2 = parameterTool.get("path2");

    SpatialDataStream<Point> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, streamPath1, new SpatialDimensionJoin.TrajectoryFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);
    SpatialDataStream<Point> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, streamPath2, new SpatialDimensionJoin.TrajectoryFlatMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1);

    DataStream<String> dataStream = pointSpatialDataStream1.spatialIntervalJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(10),
            Time.seconds(-600),
            Time.seconds(600),
            (point, point2) -> point + ", " + point2,
            new TypeHint<String>() { });
    dataStream.print();

    env.execute();
  }
}
