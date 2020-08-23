package com.github.tm.glink.index;

import com.github.tm.glink.features.GeoObject;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * @author Yu Liebing
 */
public class RTreeIndex<T extends GeoObject> extends SpatialTreeIndex<T> {

  private STRtree stRtree;

  public RTreeIndex(int nodeCapacity) {
    stRtree = new STRtree(nodeCapacity);
  }

  @Override
  public void insert(T object) {
    stRtree.insert(object.getEnvelope(), object);
  }

  @Override
  public List<T> query(Envelope envelope) {
    return stRtree.query(envelope);
  }
}
