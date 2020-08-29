package com.github.tm.glink.operator;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.operator.cluster.WindowAllDBSCAN;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author Yu Liebing
 */
public class DBSCAN {

  public static DataStream<Tuple2<Integer, Point>> dbscan(
          DataStream<Point> geoDataStream,
          int windowSize,
          double distance,
          int minPts,
          double gridWidth) {
    DataStream<Tuple2<Point, Point>> pairRangeJoinStream = PairRangeJoin.pairRangeJoin(
            geoDataStream, windowSize, distance, gridWidth);
    return pairRangeJoinStream
            .windowAll(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new WindowAllDBSCAN(minPts));
  }
}
