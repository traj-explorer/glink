package com.github.tm.glink.features.utils;

import com.github.tm.glink.features.Point;
import org.locationtech.jts.geom.Coordinate;

import java.util.Comparator;

/**
 * @author Yu Liebing
 */
@Deprecated
public class GeoDistanceComparator implements Comparator<Point> {

  private Coordinate queryPoint;

  public GeoDistanceComparator(Coordinate queryPoint) {
    this.queryPoint = queryPoint;
  }

  @Override
  public int compare(Point p1, Point p2) {
    double d1 = GeoUtil.computeGeoDistance(queryPoint, p1);
    double d2 = GeoUtil.computeGeoDistance(queryPoint, p2);
    if (Math.abs(d1 - d2) < 1e-6) {
      return 0;
    } else if (d1 > d2) {
      return 1;
    } else {
      return -1;
    }
  }
}
