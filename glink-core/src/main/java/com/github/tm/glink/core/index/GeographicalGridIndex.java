package com.github.tm.glink.core.index;

import com.github.tm.glink.features.ClassfiedGrids;
import com.github.tm.glink.features.utils.GeoUtil;
import org.apache.flink.annotation.VisibleForTesting;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Yu Liebing
 */
public class GeographicalGridIndex extends GridIndex {

  private static final int MAX_BITS = 30;

  private double latWidth;
  private double lngWidth;
  private double gridWidth;

  public GeographicalGridIndex(int res) {
    if (res < 0 || res > MAX_BITS)
      throw new IllegalArgumentException("Resolution of GridIndex must between [0, 30]");
    this.res = res;
    double splits = Math.pow(2, res);
    this.latWidth = 180.d / splits;
    this.lngWidth = 360.d / splits;
  }

  public GeographicalGridIndex(double gridWidth) {
    this.gridWidth = gridWidth;
  }

  @Override
  public int getRes() {
    return res;
  }

  @Override
  public long getIndex(double lat, double lng) {
    long x = (long) ((lat + 90.d) / latWidth);
    long y = (long) ((lng + 180.d) / lngWidth);
    return combineXY(x, y);
  }

  @Override
  public List<Long> getIndex(Geometry geom) {
    if (geom instanceof Point) {
      Point p = (Point) geom;
      long index = getIndex(p.getY(), p.getX());
      return new ArrayList<Long>(1) {{ add(index); }};
    } else {
      return getIntersectIndex(geom);
    }
  }

  @Override
  public List<Long> getRangeIndex(double lat, double lng, double distance, boolean fullMode) {
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
  public List<Long> getRangeIndex(double minLat, double minLng, double maxLat, double maxLng) {
    long minX = (long) ((minLat + 90.d) / latWidth);
    long maxX = (long) ((maxLat + 90.d) / latWidth);
    long minY = (long) ((minLng + 180.d) / lngWidth);
    long maxY = (long) ((maxLng + 180.d) / lngWidth);
    int n = (int) ((maxX - minX + 1) * (maxY - minY + 1));
    List<Long> res = new ArrayList<>(n);
    for (long i = minX; i <= maxX; ++i) {
      for (long j = minY; j <= maxY; ++j) {
        res.add(combineXY(i, j));
      }
    }
    return res;
  }

  @Override
  public List<Long> getIntersectIndex(Geometry geoObject) {
    Envelope envelope = geoObject.getEnvelopeInternal();
    long minX = (long) ((envelope.getMinX() + 90.d) / gridWidth);
    long maxX = (long) ((envelope.getMaxX() + 90.d) / gridWidth);
    long minY = (long) ((envelope.getMinY() + 180.d) / gridWidth);
    long maxY = (long) ((envelope.getMaxY() + 180.d) / gridWidth);
    List<Long> indexes = new ArrayList<>((int) ((maxX - minX + 1) * (maxY - minY + 1)));
    for (long x = minX; x <= maxX; ++x) {
      for (long y = minY; y <= maxY; ++y) {
        indexes.add(combineXY(x, y));
      }
    }
    return indexes;
  }

  @Override
  public List<Long> getContainsIndex(Geometry geom) {
    Envelope envelope = geom.getEnvelopeInternal();
    long minX = (long) ((envelope.getMinX() + 90.d) / gridWidth);
    long maxX = (long) ((envelope.getMaxX() + 90.d) / gridWidth);
    long minY = (long) ((envelope.getMinY() + 180.d) / gridWidth);
    long maxY = (long) ((envelope.getMaxY() + 180.d) / gridWidth);
    List<Long> indexes = new ArrayList<>((int) ((maxX - minX + 1) * (maxY - minY + 1)));
    for (long x = minX; x <= maxX; ++x) {
      for (long y = minY; y <= maxY; ++y) {
        long index = combineXY(x, y);
      }
    }
    return indexes;
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
