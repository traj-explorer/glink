package com.github.tm.glink.util;

import com.github.tm.glink.feature.Point;

import java.util.Comparator;

/**
 * @author Yu Liebing
 */
public class GeoDistanceComparator implements Comparator<Point> {

  private Point queryPoint;

  public GeoDistanceComparator(Point queryPoint) {
    this.queryPoint = queryPoint;
  }

  @Override
  public int compare(Point p1, Point p2) {
    double d1 = GeoUtil.computeGeoDistance(queryPoint, p1);
    double d2 = GeoUtil.computeGeoDistance(queryPoint, p2);
    if (Math.abs(d1 - d2) < 1e-6) {
      return 0;
    } else if (d1 > d2) {
      return -1;
    } else {
      return 1;
    }
  }
}
