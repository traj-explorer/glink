package com.github.tm.glink.index;

import com.github.tm.glink.features.Point;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.Test;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.index.strtree.STRtree;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.util.List;

public class RTreeIndexTest {

  @Test
  public void insertQueryTest1() {
    Envelope q1 = new Envelope(10, 15, 20, 25);
    Envelope q2 = new Envelope(40, 45, 110, 115);

    RTreeIndex<Point> pointRTree = new RTreeIndex<>(8);

    Point p0 = new Point(9.9, 19.9); pointRTree.insert(p0);
    Point p1 = new Point(10, 20); pointRTree.insert(p1);
    Point p2 = new Point(15, 25); pointRTree.insert(p2);
    Point p3 = new Point(40, 110); pointRTree.insert(p3);
    Point p4 = new Point(45, 115); pointRTree.insert(p4);

    List<Point> r1 = pointRTree.query(q1);
    List<Point> r2 = pointRTree.query(q2);

    System.out.println("result of q1");
    for (Point p : r1) {
      System.out.println(p);
    }

    System.out.println("result of q2");
    for (Point p : r2) {
      System.out.println(p);
    }
  }

  @Test
  public void insertQueryTest2() throws ParseException {
    GeometryFactory factory = new GeometryFactory();
    WKTReader wktReader = new WKTReader();

    STRtree stRtree = new STRtree(8);
    Polygon p1 = (Polygon) wktReader.read("POLYGON ((10 10, 10 20, 20 20, 20 10, 10 10))");
    Polygon p2 = (Polygon) wktReader.read("POLYGON ((15 0, 15 15, 25 15, 25 0, 15 0))");
    Polygon p3 = (Polygon) wktReader.read("POLYGON ((0 5, 5 10, 10 5, 5 0, 0 5))");
    stRtree.insert(p1.getEnvelopeInternal(), p1);
    stRtree.insert(p2.getEnvelopeInternal(), p2);
    stRtree.insert(p3.getEnvelopeInternal(), p3);

    org.locationtech.jts.geom.Point point1 = factory.createPoint(new Coordinate(18, 12));
    org.locationtech.jts.geom.Point point2 = factory.createPoint(new Coordinate(12, 12));
    org.locationtech.jts.geom.Point point3 = factory.createPoint(new Coordinate(1, 1));

    List r1 = stRtree.query(point1.getEnvelopeInternal());
    System.out.println("r1");
    for (Object o : r1) {
      System.out.println(o);
    }

    List r2 = stRtree.query(point2.getEnvelopeInternal());
    System.out.println("r2");
    for (Object o : r2) {
      System.out.println(o);
    }

    List r3 = stRtree.query(point3.getEnvelopeInternal());
    System.out.println("r3");
    for (Object o : r3) {
      System.out.println(o);
    }
  }

  @Test
  public void test() {
  }

}