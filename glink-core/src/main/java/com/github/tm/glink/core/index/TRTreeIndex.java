package com.github.tm.glink.core.index;

import com.github.davidmoten.rtree.RTree;
import com.github.davidmoten.rtree.geometry.Geometries;
import com.github.tm.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.ArrayList;
import java.util.List;

/**
 * R-Tree implementation borrow from
 * <a href="https://github.com/davidmoten/rtree">https://github.com/davidmoten/rtree</a>
 *
 * @author Yu Liebing
 * */
public class TRTreeIndex<T extends Geometry> extends TreeIndex<T> {

  private RTree<T, com.github.davidmoten.rtree.geometry.Geometry> rTree = RTree.maxChildren(3).create();

  @Override
  public void insert(List<T> geometries) {
    geometries.forEach(this::insertGeom);
  }

  @Override
  public void insert(T geom) {
    insertGeom(geom);
  }

  @Override
  public List<T> query(Envelope envelope) {
    List<T> result = new ArrayList<>();
    rTree.search(Geometries.rectangle(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY()))
            .toBlocking()
            .toIterable()
            .forEach(item -> result.add(item.value()));
    return result;
  }

  @Override
  public List<T> query(Geometry geom, double distance) {
    Point point = geom.getCentroid();
    Envelope box = GeoUtils.calcBoxByDist(point, distance);
    List<T> result = new ArrayList<>();
    rTree.search(Geometries.rectangle(box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY()))
            .toBlocking()
            .toIterable()
            .forEach(item -> {
              if (GeoUtils.calcDistance(geom, item.value()) <= distance)
                result.add(item.value());
            });
    return result;
  }

  @Override
  public void remove(Geometry geom) {

  }

  private void insertGeom(T geom) {
    Envelope box = geom.getEnvelopeInternal();
    rTree = rTree.add(geom, Geometries.rectangle(box.getMinX(), box.getMinY(), box.getMaxX(), box.getMaxY()));
  }

  @Override
  public String toString() {
    return rTree.asString();
  }
}
