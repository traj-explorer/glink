package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.operator.join.BroadcastJoinFunction;
import com.github.tm.glink.examples.common.SpatialMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Geometry;

import java.time.Duration;

/**
 * A simple example of how to use glink to perform spatial dimension join.
 *
 * @author Yu Liebing
 * */
public class SpatialDimensionJoin {

  public static void main(String[] args) throws Exception {
    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(2);

    // map function to parse the socket text
    // text format: id,wkt,time
    MapFunction<String, Geometry> mapFunction = new SpatialMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 = new SpatialDataStream<>(env, "localhost", 9000, mapFunction);
    SpatialDataStream<Geometry> spatialDataStream2 = new SpatialDataStream<>(env, "localhost", 9001, mapFunction);

    spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            new BroadcastJoinFunction<>(TopologyType.WITHIN_DISTANCE, Tuple2::new, 1),
            new TypeHint<Tuple2<Geometry, Geometry>>() { })
            .print();

    env.execute();
  }
}
