package com.github.tm.glink.features;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.Properties;

/**
 * @author Yu Liebing
 */
@Deprecated
public abstract class GeoObject {

  // non-geo attributions of a geo object
  protected Properties attributes;

  public abstract Geometry getGeometry(GeometryFactory factory);

  public abstract Envelope getEnvelope();

  public abstract Envelope getBufferedEnvelope(double distance);

  public void setAttributes(Properties attributes) {
    this.attributes = attributes;
  }

  public Properties getAttributes() {
    return attributes;
  }

}
