package com.github.tm.glink.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class STRTreeIndex<T extends Geometry> extends TreeIndex<T> {

  private STRtree stRtree;

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
}
