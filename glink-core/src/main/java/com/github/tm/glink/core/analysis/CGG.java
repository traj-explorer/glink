package com.github.tm.glink.core.analysis;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.operator.PointPairRangeJoin;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

import java.util.*;

public class CGG {

  public static void generate(SpatialDataStream<Point> pointDataStream,
                                   double distance) {
    DataStream<Tuple2<Point, Point>> pairDataStream = PointPairRangeJoin.join(pointDataStream, distance);

    pairDataStream.map(new MapFunction<Tuple2<Point, Point>, Tuple2<Integer, Integer>>() {

      @Override
      public Tuple2<Integer, Integer> map(Tuple2<Point, Point> t) throws Exception {
        Tuple a1 = (Tuple) t.f0.getUserData();
        Tuple a2 = (Tuple) t.f1.getUserData();
        return new Tuple2<>(a1.getField(0), a2.getField(0));
      }
    }).print();

    pairDataStream
            .windowAll(TumblingEventTimeWindows.of(Time.minutes(5)))
            .apply(new ConnectedGraphWindowFunction())
            .print();
  }

  public static class ConnectedGraphWindowFunction
          implements AllWindowFunction<Tuple2<Point, Point>, Object, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<Tuple2<Point, Point>> iterable,
                      Collector<Object> collector) throws Exception {
      Map<IDPoint, List<IDPoint>> graph = new HashMap<>(256);
      Set<IDPoint> set = new HashSet<>(256);
      for (Tuple2<Point, Point> t : iterable) {
        IDPoint p1 = new IDPoint(t.f0);
        IDPoint p2 = new IDPoint(t.f1);
        graph.putIfAbsent(p1, new LinkedList<>());
        graph.putIfAbsent(p2, new LinkedList<>());
        graph.get(p1).add(p2);
        graph.get(p2).add(p1);
        set.add(p1);
        set.add(p2);
      }
      while (!set.isEmpty()) {
        List<Point> subGraph = new LinkedList<>();
        IDPoint point = set.iterator().next();
        Queue<IDPoint> q = new LinkedList<>();
        q.offer(point);
        while (!q.isEmpty()) {
          int size = q.size();
          for (int i = 0; i < size; ++i) {
            IDPoint p = q.poll();
            subGraph.add(p.getPoint());
            set.remove(p);
            List<IDPoint> neighbours = graph.get(p);
            for (IDPoint idPoint : neighbours) {
              if (set.contains(idPoint)) q.offer(idPoint);
            }
          }
        }
        collector.collect(subGraph);
      }
    }
  }

  public static class IDPoint {
    private final Point point;

    public IDPoint(Point point) {
      this.point = point;
    }

    public Point getPoint() {
      return point;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      IDPoint idPoint = (IDPoint) o;
      return Objects.equals(point.getUserData(), idPoint.point.getUserData());
    }

    @Override
    public int hashCode() {
      return point.getUserData().hashCode();
    }

    @Override
    public String toString() {
      return "IDPoint{" +
              point.getUserData() +
              '}';
    }
  }
}
