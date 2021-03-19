package com.github.tm.glink.core.util;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceCalculator;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.ShapeFactory;

/**
 * @author Yu Liebing
 * */
public class GeoUtils {

  public static final SpatialContext SPATIAL_CONTEXT = SpatialContext.GEO;
  public static final DistanceCalculator DISTANCE_CALCULATOR = SPATIAL_CONTEXT.getDistCalc();
  public static final ShapeFactory SHAPE_FACTORY = SPATIAL_CONTEXT.getShapeFactory();

  public static double calcDistance(Geometry geom1, Geometry geom2) {
    org.locationtech.jts.geom.Point p1 = geom1.getCentroid();
    org.locationtech.jts.geom.Point p2 = geom2.getCentroid();
    Point point1 = SHAPE_FACTORY.pointXY(p1.getX(), p1.getY());
    Point point2 = SHAPE_FACTORY.pointXY(p2.getX(), p2.getY());
    return SPATIAL_CONTEXT.calcDistance(point1, point2) * DistanceUtils.DEG_TO_KM;
  }

  public static Envelope calcBoxByDist(org.locationtech.jts.geom.Point p, double dis) {
    Point point = SHAPE_FACTORY.pointXY(p.getX(), p.getY());
    Rectangle rect = DISTANCE_CALCULATOR.calcBoxByDistFromPt(point, dis * DistanceUtils.KM_TO_DEG, SPATIAL_CONTEXT, null);
    return new Envelope(rect.getMinX(), rect.getMaxX(), rect.getMinY(), rect.getMaxY());
  }
}
