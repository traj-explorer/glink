package com.github.tm.glink.core.index;

import com.github.tm.glink.core.util.GeoUtils;
import org.junit.Test;
import org.locationtech.jts.geom.*;

import java.util.List;

public class UGridIndexTest {

  private final UGridIndex gridIndex = new UGridIndex(17);

  @Test
  public void getIndexTest() {
    long index = gridIndex.getIndex(45, 90);
    long[] xy = gridIndex.getXY(index);
    System.out.println("[" + xy[0] + ", " + xy[1] + "]");
  }

  @Test
  public void getRangeIndexTest() {
    GeometryFactory geometryFactory = new GeometryFactory();
    Point point = geometryFactory.createPoint(new Coordinate(114, 34));
    Envelope envelope = GeoUtils.calcBoxByDist(point, 1);
    List<Long> index = gridIndex.getIndex(point);
    System.out.println(index);

    List<Long> rangeIndex = gridIndex.getRangeIndex(envelope.getMinY(), envelope.getMinX(), envelope.getMaxY(), envelope.getMaxX());
    System.out.println(rangeIndex);
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