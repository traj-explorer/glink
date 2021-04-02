package com.github.tm.glink.core.index;

import com.github.tm.glink.features.ClassfiedGrids;
import org.apache.flink.annotation.VisibleForTesting;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class GeometricGridIndex extends GridIndex {

  private static final int MAX_BITS = 30;

  private double minX;
  private double maxX;
  private double minY;
  private double maxY;

  private double xStep;
  private double yStep;

  public GeometricGridIndex(int res, Envelope box) {
    if (res < 0 || res > MAX_BITS)
      throw new IllegalArgumentException("Resolution of GridIndex must between [0, 30]");
    this.res = res;
    minX = box.getMinX();
    maxX = box.getMaxX();
    minY = box.getMinY();
    maxY = box.getMaxY();
    double splits = Math.pow(2, res);
    xStep = (maxX - minX) / splits;
    yStep = (maxY - minY) / splits;
  }

  @Override
  public int getRes() {
    return res;
  }

  @Override
  public long getIndex(double x, double y) {
    long xV = (long) (x / xStep);
    long yV = (long) (y / yStep);
    return combineXY(xV, yV);
  }

  @Override
  public List<Long> getIndex(Geometry geom) {
    return null;
  }

  @Override
  public List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode) {
    return null;
  }

  @Override
  public List<Long> getRangeIndex(double minLat, double minLng, double maxLat, double maxLng) {
    return null;
  }

  @Override
  public List<Long> getIntersectIndex(Geometry geoObject) {
    return null;
  }

  @Override
  public void getGeoBoundary(long index) {

  }

  @Override
  public long getParent(long index) {
    return 0;
  }

  @Override
  public long getParent(long index, int res) {
    return 0;
  }

  @Override
  public List<Long> getChildren(long index) {
    return null;
  }

  @Override
  public List<Long> getChildren(long index, int res) {
    return null;
  }

  @Override
  public ClassfiedGrids getRelatedGrids(Geometry geometry) {
    return null;
  }

  @Override
  public List<Long> kRing(long index, int k) {
    return null;
  }

  private long combineXY(long x, long y) {
    return x << MAX_BITS | y;
  }

  @VisibleForTesting
  public long[] getXY(long index) {
    long y = index & 0x3fffffff;
    long x = index >> MAX_BITS;
    return new long[] {x, y};
  }
}
