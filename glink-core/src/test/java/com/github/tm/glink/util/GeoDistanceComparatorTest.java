package com.github.tm.glink.util;

import com.github.tm.glink.feature.Point;
import com.github.tm.glink.feature.Coordinate;
import org.junit.Test;

import java.util.PriorityQueue;

public class GeoDistanceComparatorTest {

  @Test
  public void test() {
    Coordinate queryPoint = new Coordinate(10., 20.);
    PriorityQueue<Point> priorityQueue = new PriorityQueue<>(2, new GeoDistanceComparator(queryPoint));
    Point p1 = new Point(15., 25.);
    Point p2 = new Point(20., 30.);
    priorityQueue.add(p1);
    priorityQueue.add(p2);

    for (Point p : priorityQueue) {
      System.out.println(p);
    }
  }

}