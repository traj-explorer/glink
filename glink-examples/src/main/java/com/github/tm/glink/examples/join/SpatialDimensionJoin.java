package com.github.tm.glink.examples.join;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.examples.utils.BroadcastFlatMapFunction;
import com.github.tm.glink.examples.utils.SpatialFlatMapFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.locationtech.jts.geom.Geometry;

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
    FlatMapFunction<String, Geometry> flatMapFunction = new SpatialFlatMapFunction();
    FlatMapFunction<String, Tuple2<Boolean, Geometry>> broadcastFlatMapFunction = new BroadcastFlatMapFunction();

    SpatialDataStream<Geometry> spatialDataStream1 = new SpatialDataStream<>(env, "localhost", 8888, flatMapFunction);
    BroadcastSpatialDataStream<Geometry> spatialDataStream2 = new BroadcastSpatialDataStream<>(env, "localhost", 9999, broadcastFlatMapFunction);

    spatialDataStream1.spatialDimensionJoin(
            spatialDataStream2,
            TopologyType.WITHIN_DISTANCE.distance(1.0),
            Tuple2::new,
            new TypeHint<Tuple2<Geometry, Geometry>>() { })
            .print();

    env.execute();
  }
}
