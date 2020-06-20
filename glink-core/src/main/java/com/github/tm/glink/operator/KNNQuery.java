package com.github.tm.glink.operator;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.operator.judgement.NativeKNNJudgement;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;

/**
 * @author Yu Liebing
 */
public class KNNQuery {

  public static <T> DataStream<T> pointKNNQuery(DataStream<T> geoDataStream, Point queryPoint, int k) {
    return null;
  }
}
