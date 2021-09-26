package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.datastream.SpatialDataStream;
import com.github.tm.glink.core.index.GeographicalGridIndex;
import com.github.tm.glink.core.index.GridIndex;
import com.github.tm.glink.core.index.TRTreeIndex;
import com.github.tm.glink.core.index.TreeIndex;
import com.github.tm.glink.core.util.GeoUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.locationtech.jts.geom.Point;

import java.util.LinkedList;
import java.util.List;

public class PointPairRangeJoin {

  public static DataStream<Tuple2<Point, Point>> join(
          final SpatialDataStream<Point> pointDataStream,
          final double distance) {
    GridIndex gridIndex = SpatialDataStream.gridIndex;

    DataStream<Tuple3<Boolean, Long, Point>> redundantStream =
            pointDataStream.getDataStream().flatMap(new FlatMapFunction<Point, Tuple3<Boolean, Long, Point>>() {
              @Override
              public void flatMap(Point point, Collector<Tuple3<Boolean, Long, Point>> collector) throws Exception {
                long index = gridIndex.getIndex(point.getX(), point.getY());
                collector.collect(new Tuple3<>(false, index, point));

                List<Long> indices = gridIndex.getIndex(point.getX(), point.getY(), distance, true);
                for (Long idx : indices) {
                  if (idx == index) {
                    continue;
                  }
                  collector.collect(new Tuple3<>(true, idx, point));
                }
              }
            });

//    redundantStream.map(new MapFunction<Tuple3<Boolean, Long, Point>, Tuple3<Boolean, Long, Object>>() {
//      @Override
//      public Tuple3<Boolean, Long, Object> map(Tuple3<Boolean, Long, Point> t) throws Exception {
//        return new Tuple3<>(t.f0, t.f1, t.f2.getUserData());
//      }
//    }).print();

    return redundantStream
            .keyBy(t -> t.f1)
            .window(TumblingEventTimeWindows.of(Time.minutes(5)))
            .apply(new PairRangeJoinWindowFunction(distance));
  }

  public static class PairRangeJoinWindowFunction
          implements WindowFunction<Tuple3<Boolean, Long, Point>, Tuple2<Point, Point>, Long, TimeWindow> {

    private double distance;

    public PairRangeJoinWindowFunction(double distance) {
      this.distance = distance;
    }

    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple3<Boolean, Long, Point>> iterable,
                      Collector<Tuple2<Point, Point>> collector) throws Exception {
      if (key == 0) {
        System.out.println();
      }
      TreeIndex<Point> treeIndex = new TRTreeIndex<>();
      List<Point> queryList = new LinkedList<>();
      for (Tuple3<Boolean, Long, Point> t : iterable) {
        if (t.f0) {
          queryList.add(t.f2);
          continue;
        }
        List<Point> queryResult = treeIndex.query(GeoUtils.calcEnvelopeByDis(t.f2, distance));
        for (Point p : queryResult) {
          collector.collect(new Tuple2<>(t.f2, p));
        }
        treeIndex.insert(t.f2);
      }

      for (Point p : queryList) {
        List<Point> queryResult = treeIndex.query(GeoUtils.calcEnvelopeByDis(p, distance));
        for (Point pp : queryResult) {
          collector.collect(new Tuple2<>(p, pp));
        }
      }
    }
  }
}
