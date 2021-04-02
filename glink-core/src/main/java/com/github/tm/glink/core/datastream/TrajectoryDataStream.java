package com.github.tm.glink.core.datastream;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * @author Yu Liebing
 * */
public class TrajectoryDataStream<T> {

  private DataStream<Tuple2<Long, T>> trajectoryDataStream;

  public TrajectoryDataStream<T> mapMatch(TrajectoryDataStream<T> trajectoryDataStream) {
    return this;
  }
}
