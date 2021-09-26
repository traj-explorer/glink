package com.github.tm.glink.core.operator.process;

import com.github.tm.glink.core.index.GeographicalGridIndex;
import com.github.tm.glink.core.index.GridIndex;
import com.github.tm.glink.features.Point;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * Assign one or more indexes for points. When the point locates near the edges of grid it located at, it will also be
 * assigned with a index of the neighbor grid. As for exact data broadcast mechanism, see {@link GeographicalGridIndex}.
 * @author Yu Liebing
 */
@Deprecated
public class RangeJoinIndexAssigner extends RichFlatMapFunction<Point, Tuple3<Boolean, Long, Point>> {

  private double distance;
  private double gridWidth;
  private boolean fullMode;
  private transient GridIndex gridIndex;

  /**
   * Init a pair range join index assigner based on {@link GeographicalGridIndex}
   * @param distance The distance(meters) from 2 points to be regarded as a pair.
   * @param gridWidth The original width of grids.
   * @param fullMode 暂时没用.
   */
  public RangeJoinIndexAssigner(double distance, double gridWidth, boolean fullMode) {
    this.distance = distance;
    this.gridWidth = gridWidth;
    this.fullMode = fullMode;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    gridIndex = new GeographicalGridIndex(gridWidth);
  }

  /**
   * @param collector To collect index results composed of 3 items:
   *                  1: Boolean, ture if the output indicate a "broadcast" to a neighbor grid.
   *                  2: Long, the index which the output will be keyed by. Note: the index DO NOT represents
   *                  the actual grid where the point locates.
   *                  3: Point, the original point object.
   */
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
