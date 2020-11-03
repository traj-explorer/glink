package com.github.tm.glink.index;

import com.github.tm.glink.features.GeoObject;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.strtree.STRtree;

import java.util.List;

/**
 * A {@link SpatialTreeIndex} implement based on {@link STRtree}. It's not recommended to add/remove items once the tree
 * is initialized.
 * @author Yu Liebing
 */
public class RTreeIndex<T extends GeoObject> extends SpatialTreeIndex<T> {

  private STRtree stRtree;

  /**
   * Construct a R-tree with the given maximum number of child nodes that a node may have.
   */
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
