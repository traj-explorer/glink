package com.github.tm.glink.index;

import com.github.tm.glink.fearures.ClassfiedGrids;
import com.github.tm.glink.fearures.utils.GeoUtil;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class UGridIndex extends GridIndex {

  private static final int MAX_BITS = 30;

  private double gridWidth;

  public UGridIndex(double gridWidth) {
    this.gridWidth = gridWidth;
  }

  @Override
  public int getRes() {
    return res;
  }

  @Override
  public long getIndex(double lat, double lng) {
    long x = (long) ((lat + 90.d) / gridWidth);
    long y = (long) ((lng + 180.d) / gridWidth);
    return combineXY(x, y);
  }

  @Override
  public List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode) {
    Coordinate upper = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 0, distance);
    Coordinate right = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 90, distance);
    Coordinate bottom = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 180, distance);
    Coordinate left = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 270, distance);
    long minX = fullMode ? (long) ((bottom.getX() + 90.d) / gridWidth) : (long) ((lat + 90.d) / gridWidth);
    long maxX = (long) ((upper.getX() + 90.d) / gridWidth);
    long minY = (long) ((left.getY() + 180.d) / gridWidth);
    long maxY = (long) ((right.getY() + 180.d) / gridWidth);
    List<Long> res = new ArrayList<>();
    for (long x = minX; x <= maxX; ++x) {
      for (long y = minY; y <= maxY; ++y) {
        res.add(combineXY(x, y));
      }
    }
    return res;
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
  public ClassfiedGrids getContainGrids(Geometry geometry) {
    return null;
  }

  @Override
  public List<Long> kRing(long index, int k) {
    return null;
  }

  private long combineXY(long x, long y) {
    return x << MAX_BITS | y;
  }
}
