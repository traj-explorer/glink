package com.github.tm.glink.core.util;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.util.Random;

import static org.junit.Assert.*;

public class GeoUtilsTest {

  private GeometryFactory factory = new GeometryFactory();

  @Test
  public void calcDistanceTest() {
    Point p1 = factory.createPoint(new Coordinate(114, 34));
    Point p2 = factory.createPoint(new Coordinate(115, 35));
    double dis = GeoUtils.calcDistance(p1, p2);
    System.out.println(dis);
  }

  @Test
  public void calcBoxByDistTest() {
    Point p = factory.createPoint(new Coordinate(114, 34));
    Envelope envelope = GeoUtils.calcBoxByDist(p, 0.1);
    System.out.println(envelope);
  }

  @Test
  public void generateRandomDataForDistanceJoin() {
    Point p1 = factory.createPoint(new Coordinate(114, 34));
    Envelope envelope = GeoUtils.calcBoxByDist(p1, 1);
    double lngStep = envelope.getMaxX() - envelope.getMinX();
    double latStep = envelope.getMaxY() - envelope.getMinY();
    Random random = new Random();
    for (int i = 1; i <= 5; ++i) {
      double lat = envelope.getMinY() + random.nextDouble() * latStep;
      double lng = envelope.getMinX() + random.nextDouble() * latStep;
      Point point = factory.createPoint(new Coordinate(lng, lat));
      if (GeoUtils.calcDistance(point, p1) > 1) continue;
      System.out.println(lat + "," + lng);
    }
  }

}