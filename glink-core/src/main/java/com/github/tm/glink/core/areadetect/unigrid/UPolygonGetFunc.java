package com.github.tm.glink.core.areadetect.unigrid;

import com.github.tm.glink.core.areadetect.PolygonGetFunc;
import com.github.tm.glink.core.index.GeographicalGridIndex;
import org.locationtech.jts.geom.Polygon;

public class UPolygonGetFunc<T> extends PolygonGetFunc<T> {

  private final GeographicalGridIndex index;

  public UPolygonGetFunc(GeographicalGridIndex index) {
    this.index = index;
  }

  @Override
  public Polygon getUnitPolygon(Long id) {
    return index.getGridPolygon(id);
  }
}
