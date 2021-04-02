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

}