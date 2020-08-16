package com.github.tm.glink.operator.process;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.index.RTreeIndex;
import com.github.tm.glink.index.SpatialTreeIndex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class AllIndexRangeJoinProcess extends RichWindowFunction<Tuple2<Boolean, Point>, List<Point>, Long, TimeWindow> {

  private static final int RTREE_NODE_CAPACITY = 50;

  private double distance;

  public AllIndexRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @Override
  public void apply(Long aLong,
                    TimeWindow timeWindow,
                    Iterable<Tuple2<Boolean, Point>> iterable,
                    Collector<List<Point>> collector) throws Exception {
    SpatialTreeIndex<Point> treeIndex = new RTreeIndex<>(RTREE_NODE_CAPACITY);
    // build index
    List<Point> points = new ArrayList<>();
    for (Tuple2<Boolean, Point> t : iterable) {
      if (!t.f0) {
        points.add(t.f1);
      }
      treeIndex.insert(t.f1);
    }
    // query
    for (Point p : points) {
      List<Point> bufferedPoints = new ArrayList<>();
      bufferedPoints.add(p);
      bufferedPoints.addAll(treeIndex.query(p.getBufferedEnvelope(distance)));
      collector.collect(bufferedPoints);
    }
  }
}
