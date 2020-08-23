package com.github.tm.glink.index;

import com.github.tm.glink.features.Point;
import org.junit.Test;
import org.locationtech.jts.geom.Envelope;

import java.util.List;

public class QuadTreeIndexTest {

  private SpatialTreeIndex<Point> treeIndex = new QuadTreeIndex<>();

  @Test
  public void insertQueryTest() {
    Envelope q1 = new Envelope(10, 15, 20, 25);
    Envelope q2 = new Envelope(40, 45, 110, 115);

    List<Point> r0 = treeIndex.query(q1);
    System.out.println("result of q0");
    for (Point p : r0) {
      System.out.println(p);
    }

    Point p0 = new Point(9.9, 19.9); treeIndex.insert(p0);
    Point p1 = new Point(10, 20); treeIndex.insert(p1);
    Point p2 = new Point(15, 25); treeIndex.insert(p2);
    Point p3 = new Point(40, 110); treeIndex.insert(p3);
    Point p4 = new Point(45, 115); treeIndex.insert(p4);

    List<Point> r1 = treeIndex.query(q1);
    List<Point> r2 = treeIndex.query(q2);

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