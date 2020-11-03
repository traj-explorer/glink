package com.github.tm.glink.operator;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.operator.process.AllIndexRangeJoinProcess;
import com.github.tm.glink.operator.process.RangeJoinIndexAssigner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;

/**
 * Perform range join on all events. It is done by following steps:
 * 1. Partition the points into several parallel sub-operators according to location, and may with some redundancy,
 *  when the point locates near the edges of grid it located at, it will also be assigned with a index of the
 *  neighbor grid.
 * 2. Window the indexed data stream in the style of tumbling time window. 将轨迹点window至时长为window size的TumblingEventTimewindow.
 * 3. Construct R-tree with data collected by the window, iterate points and generate point-centered square as
 *  query ranges, each outputs a list of points as the range join result of the center point.
 * Note: this operator get pair results in redundant.
 * @author Yu Liebing
 */
public class AllRangeJoin {
  /**
   *
   * @param geoDataStream
   * @param windowSize
   * @param distance
   * @param gridWidth
   * @return
   */
  public static DataStream<List<Point>> allRangeJoin(
          DataStream<Point> geoDataStream,
          int windowSize,
          double distance,
          double gridWidth) {
    return geoDataStream
            .flatMap(new RangeJoinIndexAssigner(distance, gridWidth, true))
            .keyBy(p -> p.f1)
            .window(TumblingEventTimeWindows.of(Time.seconds(windowSize)))
            .apply(new AllIndexRangeJoinProcess(distance));
  }
}
