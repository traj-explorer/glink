package com.github.tm.glink.core.index;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.List;

public class UGridIndexTest {

  private GridIndex gridIndex = new UGridIndex(90.d);

  @Test
  public void getIndexTest() {
    long index = gridIndex.getIndex(1, 1);
    System.out.println(index);
    System.out.println(index >> 30);
  }

  @Test
  public void getRangeIndexTest() {
    List<Long> indexes = gridIndex.getRangeIndex(0, 0, 500, false);
    for (long index : indexes) {
      System.out.println(index);
    }
  }

  @Test
  public void getIntersectIndexTest() {
    GeometryFactory factory = new GeometryFactory();
    Coordinate[] cs = new Coordinate[4];
    cs[0] = new Coordinate(10, 20);
    cs[1] = new Coordinate(100, 25);
    cs[2] = new Coordinate(50, 30);
    cs[3] = cs[0];
    Geometry geometry = factory.createPolygon(cs);
    System.out.println(geometry.getEnvelopeInternal());
    List<Long> indexes = gridIndex.getIntersectIndex(geometry);
    for (long index : indexes) {
      System.out.println(index);
    }
  }

}