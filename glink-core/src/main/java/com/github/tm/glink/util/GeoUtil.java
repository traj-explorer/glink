package com.github.tm.glink.util;

import com.github.tm.glink.feature.Point;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;

/**
 * @author Yu Liebing
 */
public class GeoUtil {

  public static double computeGeoDistance(Point p1, Point p2) {
    GlobalCoordinates c1 = new GlobalCoordinates(p1.getLat(), p1.getLng());
    GlobalCoordinates c2 = new GlobalCoordinates(p2.getLat(), p2.getLng());

    GeodeticCurve geodeticCurve = new GeodeticCalculator().calculateGeodeticCurve(Ellipsoid.WGS84, c1, c2);
    return geodeticCurve.getEllipsoidalDistance();
  }
}
