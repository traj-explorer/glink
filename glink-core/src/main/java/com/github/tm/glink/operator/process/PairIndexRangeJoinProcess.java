package com.github.tm.glink.operator.process;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.index.QuadTreeIndex;
import com.github.tm.glink.index.SpatialTreeIndex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class PairIndexRangeJoinProcess
        extends RichWindowFunction<Tuple3<Boolean, Long, Point>, Tuple2<Point, Point>, Long, TimeWindow> {

  private double distance;

  public PairIndexRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @Override
  public void apply(Long aLong,
                    TimeWindow window,
                    Iterable<Tuple3<Boolean, Long, Point>> iterable,
                    Collector<Tuple2<Point, Point>> collector) throws Exception {
    SpatialTreeIndex<Point> treeIndex = new QuadTreeIndex<>();
    List<Point> queryList = new ArrayList<>();
    for (Tuple3<Boolean, Long, Point> t : iterable) {
      if (t.f0) {
        queryList.add(t.f2);
        continue;
      }
      List<Point> queryResult = treeIndex.query(t.f2.getBufferedEnvelope(distance));
      for (Point p : queryResult) {
        collector.collect(new Tuple2<>(t.f2, p));
      }
      treeIndex.insert(t.f2);
    }

    for (Point t : queryList) {
      List<Point> queryResult = treeIndex.query(t.getBufferedEnvelope(distance));
      for (Point p : queryResult) {
        collector.collect(new Tuple2<>(t, p));
      }
    }
  }
}
