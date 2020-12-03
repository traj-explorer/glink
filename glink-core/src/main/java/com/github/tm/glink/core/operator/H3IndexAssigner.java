package com.github.tm.glink.core.operator;

import com.github.tm.glink.core.index.H3Index;
import com.github.tm.glink.features.Point;
import com.github.tm.glink.core.index.GridIndex;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Assign H3Index for coming points.
 * @author Yu Liebing
 */
public class H3IndexAssigner extends RichMapFunction<Point, Point> {

  private int res;
  private transient GridIndex gridIndex;

  public H3IndexAssigner(int res) {
    this.res = res;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    gridIndex = new H3Index(res);
  }

  @Override
  public Point map(Point point) throws Exception {
    point.setIndex(gridIndex.getIndex(point.getLat(), point.getLng()));
    return point;
  }
}
