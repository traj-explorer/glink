package com.github.tm.glink.features.util;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.utils.GeoUtil;
import org.junit.Test;

public class GeoUtilTest {

  @Test
  public void computeGeoDistanceTest() {
    Point p1 = new Point(29.490295, 106.486654);
    Point p2 = new Point(29.615467, 106.581515);
    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; ++i) {
      double dis = GeoUtil.computeGeoDistance(p1, p2);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
//    System.out.println(dis);
  }
}