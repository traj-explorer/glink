package com.github.tm.glink.core.process;

import com.github.tm.glink.core.datastream.BroadcastSpatialDataStream;
import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.enums.TopologyType;
import com.github.tm.glink.core.index.TreeIndex;
import com.github.tm.glink.core.operator.join.BroadcastJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.locationtech.jts.geom.Geometry;

public class SpatialDimensionJoin {

  /**
   * Spatial dimension join with a broadcast stream.
   *
   * @param joinStream   a {@link BroadcastSpatialDataStream} to join with
   * @param joinType     join type
   * @param joinFunction the join function
   * @param returnType   the return type of join
   */
  public static <T1 extends Geometry, T2 extends Geometry, OUT> DataStream<OUT> join(
          SpatialDataStream<T1> spatialDataStream,
          BroadcastSpatialDataStream<T2> joinStream,
          TopologyType joinType,
          JoinFunction<T1, T2, OUT> joinFunction,
          TypeHint<OUT> returnType) {
    DataStream<T1> dataStream1 = spatialDataStream.getDataStream();
    DataStream<Tuple2<Boolean, T2>> dataStream2 = joinStream.getDataStream();
    final MapStateDescriptor<Integer, TreeIndex<T2>> broadcastDesc = new MapStateDescriptor<>(
            "broadcast-state-for-dim-join",
            TypeInformation.of(Integer.class),
            TypeInformation.of(new TypeHint<TreeIndex<T2>>() {
            }));
    BroadcastStream<Tuple2<Boolean, T2>> broadcastStream = dataStream2.broadcast(broadcastDesc);
    BroadcastJoinFunction<T1, T2, OUT> broadcastJoinFunction = new BroadcastJoinFunction<>(joinType, joinFunction);
    broadcastJoinFunction.setBroadcastDesc(broadcastDesc);
    return dataStream1
            .connect(broadcastStream)
            .process(broadcastJoinFunction)
            .returns(returnType);
  }
}
