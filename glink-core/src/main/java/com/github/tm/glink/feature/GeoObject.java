package com.github.tm.glink.feature;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * @author Yu Liebing
 */
public abstract class GeoObject {

  public abstract Geometry getGeometry(GeometryFactory factory);

}
