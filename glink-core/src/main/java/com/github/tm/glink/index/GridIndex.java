package com.github.tm.glink.index;

import com.github.tm.glink.features.ClassfiedGrids;
import org.locationtech.jts.geom.Geometry;

import java.io.Serializable;
import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class GridIndex implements Serializable {

  protected int res;

  public abstract int getRes();

  public abstract long getIndex(double lat, double lng);

  public abstract List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode);

  public abstract List<Long> getIntersectIndex(Geometry geoObject);

  public abstract void getGeoBoundary(long index);

  public abstract long getParent(long index);

  public abstract long getParent(long index, int res);

  public abstract List<Long> getChildren(long index);

  public abstract List<Long> getChildren(long index, int res);

  public abstract ClassfiedGrids getRelatedGrids(Geometry geometry);

  public abstract List<Long> kRing(long index, int k);
}
