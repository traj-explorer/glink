package com.github.tm.glink.index;

import org.locationtech.jts.geom.Envelope;

import java.util.List;

/**
 * Base class of in memory tree index for spatial objects.
 * @author Yu Liebing
 */
public abstract class SpatialTreeIndex<T> {

  public abstract void insert(T object);

  /**
   * Get a list of objects contained in the input envelope.
   * @param envelope Envelope.
   * @return Query results.
   */
  public abstract List<T> query(Envelope envelope);
}
