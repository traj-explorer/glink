package com.github.tm.glink.index;

import com.github.tm.glink.features.GeoObject;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.index.quadtree.Quadtree;

import java.util.ArrayList;
import java.util.List;

public class QuadTreeIndex<T extends GeoObject> extends SpatialTreeIndex<T> {

  private Quadtree quadtree;

  public QuadTreeIndex() {
    quadtree = new Quadtree();
  }

  @Override
  public void insert(T object) {
    quadtree.insert(object.getEnvelope(), object);
  }

  @SuppressWarnings("checkstyle:NeedBraces")
  @Override
  public List<T> query(Envelope envelope) {
    List candidates = quadtree.query(envelope);
    List<T> res = new ArrayList<>(candidates.size());
    for (Object o : candidates) {
      T p = (T) o;
      if (envelope.contains(p.getEnvelope()))
        res.add(p);
    }
    return res;
  }
}
