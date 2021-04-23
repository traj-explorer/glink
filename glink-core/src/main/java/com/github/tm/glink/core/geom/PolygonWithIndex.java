package com.github.tm.glink.core.geom;

import com.github.tm.glink.core.index.STRTreeIndex;
import com.github.tm.glink.core.index.TreeIndex;
import org.locationtech.jts.geom.*;

import java.util.Arrays;
import java.util.List;

/**
 * @author Yu Liebing
 * */
public final class PolygonWithIndex extends Polygon {

  private final double maxX = Double.MAX_VALUE;
  private final TreeIndex<LineString> edges = new STRTreeIndex<>();

  private PolygonWithIndex(LinearRing shell, LinearRing[] holes, GeometryFactory factory) {
    super(shell, holes, factory);
    Coordinate[] cs = this.getCoordinates();
    // create an R-Tree for edges
    for (int i = 0; i < cs.length; ++i) {
      if (i == cs.length - 1) {
        edges.insert(factory.createLineString(new Coordinate[] {cs[i], cs[0]}));
      } else {
        edges.insert(factory.createLineString(new Coordinate[] {cs[i], cs[i + 1]}));
      }
    }
  }

  @Override
  public boolean contains(Geometry g) {
    if (g instanceof Point) {
      Point p = (Point) g;
      if (!this.getEnvelopeInternal().contains(p.getEnvelopeInternal()))
        return false;
      Coordinate c1 = p.getCoordinate();
      Coordinate c2 = new Coordinate(maxX, p.getY());
      LineString query = factory.createLineString(new Coordinate[] {c1, c2});
      List<LineString> potentialEdges = edges.query(query);
      potentialEdges.removeIf(edge -> !edge.intersects(query));
      return potentialEdges.size() != 0 && potentialEdges.size() % 2 == 1;
    } else {
      return super.contains(g);
    }
  }

  public static PolygonWithIndex fromPolygon(Polygon polygon) {
    LinearRing shell = (LinearRing) polygon.getExteriorRing();
    LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
    Arrays.setAll(holes, polygon::getInteriorRingN);
    PolygonWithIndex polygonWithIndex = new PolygonWithIndex(shell, holes, polygon.getFactory());
    polygonWithIndex.setUserData(polygon.getUserData());
    return polygonWithIndex;
  }
}
