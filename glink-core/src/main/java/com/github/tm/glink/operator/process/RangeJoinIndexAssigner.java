package com.github.tm.glink.operator.process;

import com.github.tm.glink.fearures.Point;
import com.github.tm.glink.index.GridIndex;
import com.github.tm.glink.index.UGridIndex;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class RangeJoinIndexAssigner extends RichFlatMapFunction<Point, Tuple3<Boolean, Long, Point>> {

  private double distance;
  private double gridWidth;
  private boolean fullMode;
  private transient GridIndex gridIndex;

  public RangeJoinIndexAssigner(double distance, double gridWidth, boolean fullMode) {
    this.distance = distance;
    this.gridWidth = gridWidth;
    this.fullMode = fullMode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    gridIndex = new UGridIndex(gridWidth);
  }

  @Override
  public void flatMap(Point point, Collector<Tuple3<Boolean, Long, Point>> collector) throws Exception {
    long index = gridIndex.getIndex(point.getLat(), point.getLng());
    collector.collect(new Tuple3<>(false, index, point));

    List<Long> rangeIndexes = gridIndex.getRangeIndex(point.getLat(), point.getLng(), distance, fullMode);
    for (long idx : rangeIndexes) {
      if (idx == index) {
        continue;
      }
      collector.collect(new Tuple3<>(true, idx, new Point(
              point.getId(), point.getLat(), point.getLng(), point.getTimestamp())));
    }
  }
}
