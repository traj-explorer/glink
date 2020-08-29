package com.github.tm.glink.features;

import com.github.tm.glink.features.utils.GeoUtil;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

public class PointTest {

  @Test
  public void pointTest() {
    Point p = new Point(30.674977620000002, 104.11740999999999);
    System.out.println(p.getLat());
    System.out.println(p.getLng());
    System.out.println(p);
  }

  @Test
  public void getEnvelopeTest() {
    Point p = new Point(11.1, 22.2);
    System.out.println(p.getEnvelope());
  }

  @Test
  public void getBufferedEnvelopeTest() {
    Point p = new Point(11.1, 22.2);
    double distance = 500;
    Envelope envelope = p.getBufferedEnvelope(distance);
    double leftDis = GeoUtil.computeGeoDistance(new Coordinate(p.getLat(), envelope.getMinY()), p);
    double rightDis = GeoUtil.computeGeoDistance(new Coordinate(p.getLat(), envelope.getMaxY()), p);
    double upperDis = GeoUtil.computeGeoDistance(new Coordinate(envelope.getMaxX(), p.getLng()), p);
    double bottomDis = GeoUtil.computeGeoDistance(new Coordinate(envelope.getMinX(), p.getLng()), p);
    System.out.println(leftDis);
    System.out.println(rightDis);
    System.out.println(upperDis);
    System.out.println(bottomDis);
  }

  @Test
  public void hashTest() {
    Point p1 = new Point("1", 11.28, 119.67000011, 123456, 10000);
    Point p2 = new Point("1", 11.28, 119.67000012, 123456, 10000);
    System.out.println(p1.hashCode());
    System.out.println(p2.hashCode());
    System.out.println(p1.equals(p2));
  }
}