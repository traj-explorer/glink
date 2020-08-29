package com.github.tm.glink.features.util;

import com.github.tm.glink.features.Point;
import com.github.tm.glink.features.utils.GeoUtil;
import org.junit.Test;

public class GeoUtilTest {

  @Test
  public void computeGeoDistanceTest() {
    Point p1 = new Point(35.126670, 115.062254);
    Point p2 = new Point(35.122498, 115.217914);
    double dis = 0.;
    long start = System.currentTimeMillis();
    for (int i = 0; i < 1000; ++i) {
      dis = GeoUtil.computeGeoDistance(p1, p2);
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
    System.out.println(dis);
  }
}