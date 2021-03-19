package com.github.tm.glink.core.index;

import com.github.tm.glink.core.util.GeoUtils;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class STRTreeIndex<T extends Geometry> extends TreeIndex<T> {

  private final STRtree stRtree;

  public STRTreeIndex(int nodeCapacity) {
    stRtree = new STRtree(nodeCapacity);
  }

  @Override
  public void insert(List<T> geoms) {
    for (T geom : geoms) {
      stRtree.insert(geom.getEnvelopeInternal(), geom);
    }
  }

  @Override
  public void insert(T geom) {
    stRtree.insert(geom.getEnvelopeInternal(), geom);
  }

  @Override
  public List<T> query(Envelope envelope) {
    return stRtree.query(envelope);
  }

  @Override
  public List<T> query(Geometry geometry, double distance) {
    Point point = geometry.getCentroid();
    Envelope envelope = GeoUtils.calcBoxByDist(point, distance);
    List<T> result = stRtree.query(envelope);
    result.removeIf(geom -> GeoUtils.calcDistance(point, geom) > distance);
    return result;
  }
}
