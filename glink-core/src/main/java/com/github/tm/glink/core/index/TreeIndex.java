package com.github.tm.glink.core.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;

import java.util.List;

/**
 * @author Yu Liebing
 */
public abstract class TreeIndex<T extends Geometry> {

  public abstract void insert(List<T> geometries);

  public abstract void insert(T geom);

  public abstract List<T> query(Envelope envelope);

  public abstract List<T> query(Geometry geom, double distance);

  public abstract void remove(Geometry geom);
}
