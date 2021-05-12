package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.index.GeographicalGridIndex;
import com.github.tm.glink.core.operator.cluster.WindowAllDBSCAN;
import com.github.tm.glink.features.Point;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Process DBSCAN on all coming events. It is done by following steps:
 * 1. Get neighbor stream using {@link PairRangeJoin}.
 * 2. Window the indexed data stream in the style of tumbling time window. 将轨迹点window至时长为window size的TumblingEventTimewindow.
 * 3. Process DBSCAN and get a result data stream composed of Tuple2(Cluster_id, Point_info).
 * @author Yu Liebing
 */
public class DBSCAN {
  /**
   * Process DBSCAN on a point data stream based on a tumbling time window, now using {@link GeographicalGridIndex}
   * @param geoDataStream The input point data stream.
   * @param windowSize The window size(seconds).
   * @param distance Distance(meters) value to search for neighbors.
   * @param minPts The number of samples in a neighborhood for a point to be considered as a core point.
   * @param gridWidth 暂时没用
   * @return Cluster data stream composed of <b>Tuple2<Integer,Point></b>, where the first indicates the cluster id, and
   *  the other indicates the point information.
   */
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
