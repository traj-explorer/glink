package com.github.tm.glink.operator;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.operator.process.DbscanExpandCluster;

import java.util.List;

public class DBSCAN {
  public static DataStream<List<Point>> processDbscan(
      DataStream<Point> geoDataStream,
      int windowSize,
      double distance,
      double gridWidth,
      int minPts) {
    return PairRangeJoin
        .pairRangeJoin(geoDataStream, windowSize, distance, gridWidth)
        .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
        .process(new DbscanExpandCluster(minPts)).setParallelism(1);
  }
}
