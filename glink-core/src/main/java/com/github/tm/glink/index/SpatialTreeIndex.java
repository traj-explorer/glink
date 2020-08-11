package com.github.tm.glink.index;

import org.locationtech.jts.geom.Envelope;

import java.util.List;

/**
 * Base class of in memory tree index for spatial objects.
 *
 * @author Yu Liebing
 */
public abstract class SpatialTreeIndex<T> {

  public abstract void insert(T object);

  public abstract List<T> query(Envelope envelope);
}
