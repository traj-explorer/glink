package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.index.UGridIndex;
import com.github.tm.glink.examples.common.SpatialMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

public class SpatialIntervalJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    SpatialDataStream<Geometry> pointSpatialDataStream1 =
            new SpatialDataStream<>(env, "localhost", 8888, new SpatialMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1)
                    .assignGrids(new UGridIndex(17));
    SpatialDataStream<Geometry> pointSpatialDataStream2 =
            new SpatialDataStream<>(env, "localhost", 9999, new SpatialMapFunction())
                    .assignBoundedOutOfOrdernessWatermarks(Duration.ZERO, 1)
                    .assignGridAndDistributeByDistance(new UGridIndex(17), 10);

    DataStream<Object> dataStream = pointSpatialDataStream1.spatialIntervalJoin(
            pointSpatialDataStream2,
            TopologyType.WITHIN_DISTANCE,
            Time.seconds(-5),
            Time.seconds(5),
            (point, point2) -> point + ", " + point2,
            10);
    dataStream.print();

    env.execute();
  }
}
