package com.github.tm.glink.index;

import com.github.tm.glink.features.Point;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

public class RTreeIndexTest {

  @Test
  public void insertQueryTest() {
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

}