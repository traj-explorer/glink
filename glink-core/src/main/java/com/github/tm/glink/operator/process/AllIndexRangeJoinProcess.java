package com.github.tm.glink.operator.process;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.index.RTreeIndex;
import com.github.tm.glink.index.SpatialTreeIndex;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class AllIndexRangeJoinProcess extends RichWindowFunction<Tuple2<Boolean, Point>, List<Point>, Long, TimeWindow> {

  private static final int RTREE_NODE_CAPACITY = 50;

  private double distance;
  private transient SpatialTreeIndex<Point> treeIndex;

  public AllIndexRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    treeIndex = new RTreeIndex<>(RTREE_NODE_CAPACITY);
  }

  @Override
  public void apply(Long aLong,
                    TimeWindow timeWindow,
                    Iterable<Tuple2<Boolean, Point>> iterable,
                    Collector<List<Point>> collector) throws Exception {
    // build index
    List<Point> points = new ArrayList<>();
    for (Tuple2<Boolean, Point> t : iterable) {
      points.add(t.f1);
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
