package com.github.tm.glink.index;

import com.github.tm.glink.features.ClassfiedGrids;
import com.github.tm.glink.features.utils.GeoUtil;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

import java.util.ArrayList;
import java.util.LinkedList;
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
    //    Coordinate upper = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 0, distance);
//    Coordinate right = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 90, distance);
//    Coordinate bottom = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 180, distance);
//    Coordinate left = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 270, distance);
//    long minX = fullMode ? (long) ((bottom.getX() + 90.d) / gridWidth) : (long) ((lat + 90.d) / gridWidth);
//    long maxX = (long) ((upper.getX() + 90.d) / gridWidth);
//    long minY = (long) ((left.getY() + 180.d) / gridWidth);
//    long maxY = (long) ((right.getY() + 180.d) / gridWidth);
//    List<Long> res = new ArrayList<>();
//    for (long x = minX; x <= maxX; ++x) {
//      for (long y = minY; y <= maxY; ++y) {
//        res.add(combineXY(x, y));
//      }
//    }
//    return res;

    Coordinate upper = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 0, distance);
    Coordinate right = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 90, distance);
    Coordinate bottom = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 180, distance);
    Coordinate left = GeoUtil.calculateEndingLatLng(new Coordinate(lat, lng), 270, distance);
    long minX = (long) ((bottom.getX() + 90.d) / gridWidth);
    long maxX = (long) ((upper.getX() + 90.d) / gridWidth);
    long minY = (long) ((left.getY() + 180.d) / gridWidth);
    long maxY = (long) ((right.getY() + 180.d) / gridWidth);
    List<Long> res = new ArrayList<>();
    if (minX == maxX && minY == maxY) {
      res.add(combineXY(minX, minY));
      return res;
    } else if (minX == maxX && minY < maxY) {
      res.add(combineXY(minX, minY));
      return res;
    } else if (minX < maxX && minY == maxY) {
      res.add(combineXY(maxX, minY));
      return res;
    } else {
      long curX = (long) ((lat + 90.d) / gridWidth);
      long curY = (long) ((lng + 180.d) / gridWidth);
      if (curX == minX && curY == minY) {
        res.add(combineXY(maxX, minY));
        res.add(combineXY(maxX, maxY));
        return res;
      } else if (curX == maxX && curY == minY) {
        return res;
      } else if (curX == maxX && curY == maxY) {
        res.add(combineXY(maxX, minY));
        return res;
      } else {
        res.add(combineXY(minX, minY));
        res.add(combineXY(maxX, minY));
        res.add(combineXY(maxX, maxY));
        return res;
      }
    }
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
    ClassfiedGrids ret = new ClassfiedGrids();
    if (geometry.isRectangle()) {
      Coordinate[] vertexes = geometry.getCoordinates();
      long minX = (long) ((vertexes[0].getX() + 90.d) / gridWidth);
      long maxX = (long) ((vertexes[2].getX() + 90.d) / gridWidth);
      long minY = (long) ((vertexes[0].getY() + 180.d) / gridWidth);
      long maxY = (long) ((vertexes[2].getY() + 180.d) / gridWidth);
      // 不在矩形边缘的格子，即认为是矩形的内部。
      for (long i = minX; i < maxX - 1; ++i) {
        for (long j = minY; j < maxY - 1; ++j) {
          ret.confirmedIndexesAdd(combineXY(i, j));
        }
      }
      // 对边缘格网的处理
      for (long i = minX; i < maxX + 1; i++) {
        ret.toCheckIndexesAdd(combineXY(i, minY));
        ret.toCheckIndexesAdd(combineXY(i, maxY));
      }
      for (long i = minY + 1; i < maxY; i++) {
        ret.toCheckIndexesAdd(combineXY(minX, i));
        ret.toCheckIndexesAdd(combineXY(maxX, i));
      }
    } else {
      Coordinate[] vertexes = geometry.getEnvelope().getCoordinates();
      long minX = (long) ((vertexes[0].getX() + 90.d) / gridWidth);
      long maxX = (long) ((vertexes[2].getX() + 90.d) / gridWidth);
      long minY = (long) ((vertexes[0].getY() + 180.d) / gridWidth);
      long maxY = (long) ((vertexes[2].getY() + 180.d) / gridWidth);
      for (long i = minX; i <= maxX; i++) {
        for (long j = minY; j <= maxY; j++) {
          ret.toCheckIndexesAdd(combineXY(i, j));
        }
      }
    }
    return ret;
  }

  @Override
  public List<Long> kRing(long index, int k) {
    long x = index >> MAX_BITS;
    long y = (index << (64 - MAX_BITS)) >> (64 - MAX_BITS);
    long offsetMinX = x - k;
    long offsetMaxX = x + k;
    long offsetMinY = y - k;
    long offsetMaxY = y + k;
    List<Long> ret = new LinkedList<>();
    for (long i = offsetMinX; i < offsetMaxX + 1; i++) {
      ret.add(combineXY(i, offsetMaxY));
      ret.add(combineXY(i, offsetMinY));
    }
    for (long i = offsetMinY + 1; i < offsetMaxY; i++) {
      ret.add(combineXY(offsetMinX, i));
      ret.add(combineXY(offsetMaxX, i));
    }
    return ret;
  }

  private long combineXY(long x, long y) {
    return x << MAX_BITS | y;
  }
}
