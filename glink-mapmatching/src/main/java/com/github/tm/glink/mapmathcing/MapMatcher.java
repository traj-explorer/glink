package com.github.tm.glink.mapmathcing;

import com.github.tm.glink.features.TrajectoryPoint;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author Yu Liebing
 */
public class MapMatcher {

  public static DataStream<TrajectoryPoint> mapMatch(DataStream<TrajectoryPoint> trajectoryDataStream) {
    return trajectoryDataStream.keyBy(TrajectoryPoint::getId)
            .flatMap(new MatcherOp<>());
  }
}
