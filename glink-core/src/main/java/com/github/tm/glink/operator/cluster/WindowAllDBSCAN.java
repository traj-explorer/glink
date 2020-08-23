package com.github.tm.glink.operator.cluster;

import com.github.tm.glink.fearures.Point;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

/**
 * @author Yu Liebing
 */
public class WindowAllDBSCAN extends RichAllWindowFunction<Tuple2<Point, Point>, Tuple2<Integer, Point>, TimeWindow> {

  private int minPts;

  public WindowAllDBSCAN(int minPts) {
    this.minPts = minPts;
  }

  @Override
  public void apply(TimeWindow timeWindow,
                    Iterable<Tuple2<Point, Point>> iterable,
                    Collector<Tuple2<Integer, Point>> collector) throws Exception {
    Map<Point, List<Point>> point2Neighbours = new HashMap<>();
    for (Tuple2<Point, Point> t : iterable) {
      point2Neighbours.putIfAbsent(t.f0, new ArrayList<>());
      point2Neighbours.putIfAbsent(t.f1, new ArrayList<>());
      point2Neighbours.get(t.f0).add(t.f1);
      point2Neighbours.get(t.f1).add(t.f0);
    }

    Set<Point> visited = new HashSet<>();
    int k = 0;
    for (Map.Entry<Point, List<Point>> entry : point2Neighbours.entrySet()) {
      if (!visited.contains(entry.getKey())) {
        visited.add(entry.getKey());
        if (entry.getValue().size() >= minPts) {
          Queue<Point> q = new LinkedList<>();
          q.offer(entry.getKey());
          while (!q.isEmpty()) {
            Point p = q.poll();
            collector.collect(new Tuple2<>(k, p));
            if (point2Neighbours.get(p).size() >= minPts) {
              for (Point pn : point2Neighbours.get(p)) {
                if (!visited.contains(pn)) {
                  q.offer(pn);
                  visited.add(pn);
                }
              }
            }
          }
          k++;
        }
      }
    }
  }
}
