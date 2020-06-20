package com.github.tm.glink.util;

import com.github.tm.glink.feature.Point;
import org.junit.Test;

import static org.junit.Assert.*;

public class GeoUtilTest {

  @Test
  public void computeGeoDistanceTest() {
    Point p1 = new Point(29.490295, 106.486654);
    Point p2 = new Point(29.615467, 106.581515);
    double dis = GeoUtil.computeGeoDistance(p1, p2);
    System.out.println(dis);
  }
}