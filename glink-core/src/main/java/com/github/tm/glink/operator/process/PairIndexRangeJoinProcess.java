package com.github.tm.glink.operator.process;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.index.QuadTreeIndex;
import com.github.tm.glink.index.SpatialTreeIndex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class PairIndexRangeJoinProcess extends RichWindowFunction<Tuple2<Boolean, Point>, Tuple2<Point, Point>, Long, TimeWindow> {

  private double distance;
  private transient SpatialTreeIndex<Point> treeIndex;

  public PairIndexRangeJoinProcess(double distance) {
    this.distance = distance;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    treeIndex = new QuadTreeIndex<>();
  }

  @Override
  public void apply(Long aLong,
                    TimeWindow window,
                    Iterable<Tuple2<Boolean, Point>> iterable,
                    Collector<Tuple2<Point, Point>> collector) throws Exception {
    List<Tuple2<Boolean, Point>> list = new ArrayList<>();
    for (Tuple2<Boolean, Point> t : iterable) {
      if (t.f0) {
        list.add(t);
        continue;
      }
      List<Point> queryResult = treeIndex.query(t.f1.getBufferedEnvelope(distance));
      for (Point p : queryResult) {
        collector.collect(new Tuple2<>(t.f1, p));
      }
      treeIndex.insert(t.f1);
    }

    for (Tuple2<Boolean, Point> t : list) {
      List<Point> queryResult = treeIndex.query(t.f1.getBufferedEnvelope(distance));
      for (Point p : queryResult) {
        collector.collect(new Tuple2<>(t.f1, p));
      }
    }
  }
}
