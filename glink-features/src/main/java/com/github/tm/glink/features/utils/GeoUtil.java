package com.github.tm.glink.features.utils;

import com.github.tm.glink.features.Point;
import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;
import org.locationtech.jts.geom.Coordinate;

/**
 * @author Yu Liebing
 */
public class GeoUtil {

  public static double computeGeoDistance(Coordinate p1, Coordinate p2) {
    GlobalCoordinates c1 = new GlobalCoordinates(p1.getX(), p1.getY());
    GlobalCoordinates c2 = new GlobalCoordinates(p2.getX(), p2.getY());
    return computeGeoDistance(c1, c2);
  }

  public static double computeGeoDistance(Coordinate p1, Point p2) {
    GlobalCoordinates c1 = new GlobalCoordinates(p1.getX(), p1.getY());
    GlobalCoordinates c2 = new GlobalCoordinates(p2.getLat(), p2.getLng());
    return computeGeoDistance(c1, c2);
  }

  public static double computeGeoDistance(Point p1, Point p2) {
    GlobalCoordinates c1 = new GlobalCoordinates(p1.getLat(), p1.getLng());
    GlobalCoordinates c2 = new GlobalCoordinates(p2.getLat(), p2.getLng());
    return computeGeoDistance(c1, c2);
  }

  public static double computeGeoDistance(GlobalCoordinates p1, GlobalCoordinates p2) {
    GeodeticCurve geodeticCurve = new GeodeticCalculator().calculateGeodeticCurve(Ellipsoid.WGS84, p1, p2);
    return geodeticCurve.getEllipsoidalDistance();
  }

  public static GlobalCoordinates calculateEndingLatLng(GlobalCoordinates start, double angle, double distance) {
    return new GeodeticCalculator().calculateEndingGlobalCoordinates(
            Ellipsoid.WGS84, start, angle, distance);
  }

  public static Coordinate calculateEndingLatLng(Coordinate start, double angle, double distance) {
    GlobalCoordinates c = new GlobalCoordinates(start.getX(), start.getY());
    GlobalCoordinates end = calculateEndingLatLng(c, angle, distance);
    return new Coordinate(end.getLatitude(), end.getLongitude());
  }
}
