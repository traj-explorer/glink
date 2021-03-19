package com.github.tm.glink.core.index;

import com.github.davidmoten.rtree.Entry;
import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.davidmoten.rtree.geometry.Geometry;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import rx.Observable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class TRTreeIndexTest {

  private GeometryFactory factory = new GeometryFactory();
  private List<Point> points = new ArrayList<>();

  @Before
  public void init() {
    points.add(factory.createPoint(new Coordinate(114, 35)));
    points.add(factory.createPoint(new Coordinate(115, 34)));
    points.add(factory.createPoint(new Coordinate(100, 20)));
    points.add(factory.createPoint(new Coordinate(90, 10)));
  }

  @Test
  public void insertTest() {
    TreeIndex<Point> treeIndex = new TRTreeIndex<>();
    treeIndex.insert(points);
    System.out.println(treeIndex);
  }

  @Test
  public void rangeQueryTest() {
    TreeIndex<Point> treeIndex = new TRTreeIndex<>();
    treeIndex.insert(points);

    List<Point> result = treeIndex.query(new Envelope(110, 120, 30, 40));
    System.out.println(result);
  }

  @Test
  public void distanceQueryTest() {

  }

  @Test
  public void test() {
    GeometryFactory factory = new GeometryFactory();
    Point p1 = factory.createPoint(new Coordinate(1, 2));
    Point p2 = factory.createPoint(new Coordinate(3, 4));

    RTree<org.locationtech.jts.geom.Geometry, Geometry> tree = RTree.create();
    tree = tree
            .add(p1, Geometries.point(1, 2))
            .add(p2, Geometries.point(3, 4));
    Observable<Entry<org.locationtech.jts.geom.Geometry, Geometry>> entries = tree.search(Geometries.point(5, 5), 10);
    for (Entry<org.locationtech.jts.geom.Geometry, Geometry> e : entries.toBlocking().toIterable()) {
      System.out.println(e.value());
    }
  }

}