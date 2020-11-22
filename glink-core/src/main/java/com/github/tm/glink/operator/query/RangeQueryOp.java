package com.github.tm.glink.operator.query;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class RangeQueryOp<T extends Geometry> extends RichFilterFunction<T> {

  private final List<Polygon> polygons = new ArrayList<>();
  private transient STRtree stRtree;

  @Override
  public void open(Configuration parameters) throws Exception {
    stRtree = new STRtree(2);
    for (Polygon polygon : polygons)
      stRtree.insert(polygon.getEnvelopeInternal(), polygon);
  }

  public RangeQueryOp(Polygon polygon) {
    polygons.add(polygon);
  }

  public RangeQueryOp(List<Polygon> polygons) {
    this.polygons.addAll(polygons);
  }

  @Override
  public boolean filter(T t) throws Exception {
    return false;
  }
}
