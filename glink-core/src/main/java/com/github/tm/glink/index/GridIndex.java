package com.github.tm.glink.index;

import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class GridIndex {

  public abstract long getIndex(double lat, double lng);

  public abstract long getParent(long index);

  public abstract List<Long> getChildren(long index);

  public abstract List<Long> getContainGrids(Geometry geometry);

}
