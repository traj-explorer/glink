package com.github.tm.glink.operator.process;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.index.RTreeIndex;
import com.github.tm.glink.index.SpatialTreeIndex;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * The redundant version of {@link PairIndexRangeJoinProcess}, which means it will generate
 * one pair of neighbor for 2 times, in separate lists.
 * @author Yu Liebing
 */
public class AllIndexRangeJoinProcess
        extends RichWindowFunction<Tuple3<Boolean, Long, Point>, List<Point>, Long, TimeWindow> {

  private static final int RTREE_NODE_CAPACITY = 50;

  private double distance;

  public  AllIndexRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @Override
  public void apply(Long aLong,
                    TimeWindow timeWindow,
                    Iterable<Tuple3<Boolean, Long, Point>> iterable,
                    Collector<List<Point>> collector) throws Exception {
    SpatialTreeIndex<Point> treeIndex = new RTreeIndex<>(RTREE_NODE_CAPACITY);
    // build index
    List<Point> points = new ArrayList<>();
    for (Tuple3<Boolean, Long, Point> t : iterable) {
      if (!t.f0) {
        points.add(t.f2);
      }
      treeIndex.insert(t.f2);
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
