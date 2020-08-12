package com.github.tm.glink.fearures;

import com.github.tm.glink.fearures.utils.GeoUtil;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;

public class PointTest {

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
}