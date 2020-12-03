package com.github.tm.glink.core.operator.process;

import com.github.tm.glink.core.index.QuadTreeIndex;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.index.SpatialTreeIndex;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Perform a pair range join and get the pair of neighbors without redundant.
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
      // 以本身就处于格网中的点为中心进行查询
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
    // 以被传输到该格网对他格网点为中心进行查询
    for (Point t : queryList) {
      List<Point> queryResult = treeIndex.query(t.getBufferedEnvelope(distance));
      for (Point p : queryResult) {
        collector.collect(new Tuple2<>(t, p));
      }
    }
  }
}
