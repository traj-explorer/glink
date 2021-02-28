package com.github.tm.glink.core.operator.judgement;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.utils.GeoDistanceComparator;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Coordinate;

import java.util.PriorityQueue;

/**
 * @author Yu Liebing
 */
public class NativeKNNJudgement {

  public static void windowApply(
          Coordinate queryPoint,
          int k,
          Integer windowKey,
          TimeWindow timeWindow,
          Iterable<Point> iterable,
          Collector<Point> collector) {
    long startTime = System.currentTimeMillis();

    PriorityQueue<Point> priorityQueue = new PriorityQueue<>(new GeoDistanceComparator(queryPoint));
    int cnt = 0;  // point count in this window
    for (Point p : iterable) {
      priorityQueue.add(p);
      cnt++;
    }
    int i = 0;
    while (i < k && !priorityQueue.isEmpty()) {
      collector.collect(priorityQueue.poll());
      i++;
    }

    long endTime = System.currentTimeMillis();
    // log information for debug
    System.out.println(String.format("ThreadId: %d, key: %d, point count: %d, time: %d",
            Thread.currentThread().getId(), windowKey, cnt, (endTime - startTime)));
  }

  public static class NativeKeyedKNNJudgement implements WindowFunction<Point, Point, Integer, TimeWindow> {

    private Coordinate queryPoint;
    private int k;

    public NativeKeyedKNNJudgement(Coordinate queryPoint, int k) {
      this.queryPoint = queryPoint;
      this.k = k;
    }

    @Override
    public void apply(Integer windowKey, TimeWindow timeWindow, Iterable<Point> iterable, Collector<Point> collector)
            throws Exception {
      windowApply(queryPoint, k, windowKey, timeWindow, iterable, collector);
    }
  }

  public static class NativeAllKNNJudgement implements AllWindowFunction<Point, Point, TimeWindow> {

    private Coordinate queryPoint;
    private int k;

    public NativeAllKNNJudgement(Coordinate queryPoint, int k) {
      this.queryPoint = queryPoint;
      this.k = k;
    }

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Point> iterable, Collector<Point> collector) throws Exception {
      windowApply(queryPoint, k, null, timeWindow, iterable, collector);
    }
  }
}
