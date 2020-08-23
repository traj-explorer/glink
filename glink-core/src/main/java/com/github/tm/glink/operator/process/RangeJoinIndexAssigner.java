package com.github.tm.glink.operator.process;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.index.GridIndex;
import com.github.tm.glink.index.UGridIndex;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class RangeJoinIndexAssigner extends RichFlatMapFunction<Point, Tuple2<Boolean, Point>> {

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
  /**
   * The first element is <b>true</b> while the point is a query object, and otherwise a data object.
   */
  public void flatMap(Point point, Collector<Tuple2<Boolean, Point>> collector) throws Exception {
    long index = gridIndex.getIndex(point.getLat(), point.getLng());
    point.setIndex(index);
    collector.collect(new Tuple2<>(false, point));

    List<Long> rangeIndexes = gridIndex.getRangeIndex(point.getLat(), point.getLng(), distance, fullMode);
    for (long idx : rangeIndexes) {
      if (idx == index) {
        continue;
      }
      collector.collect(new Tuple2<>(true, new Point(
              point.getId(), point.getLat(), point.getLng(), point.getTimestamp(), idx)));
    }
  }
}
