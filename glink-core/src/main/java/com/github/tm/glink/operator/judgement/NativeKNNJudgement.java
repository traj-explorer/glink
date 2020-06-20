package com.github.tm.glink.operator.judgement;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.util.GeoDistanceComparator;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

/**
 * @author Yu Liebing
 */
public class NativeKNNJudgement implements WindowFunction<Point, List<Point>, String, TimeWindow> {

  private Point queryPoint;
  private int k;

  public NativeKNNJudgement(Point queryPoint, int k) {
    this.queryPoint = queryPoint;
    this.k = k;
  }

  @Override
  public void apply(String s, TimeWindow timeWindow, Iterable<Point> iterable, Collector<List<Point>> collector)
          throws Exception {
    PriorityQueue<Point> priorityQueue = new PriorityQueue<>(new GeoDistanceComparator(queryPoint));
    for (Point p : iterable) {
      priorityQueue.add(p);
    }
    List<Point> result = new ArrayList<>(k);
    for (int i = 0; i < k; ++i) {
      result.add(priorityQueue.poll());
    }
    collector.collect(result);
  }
}
