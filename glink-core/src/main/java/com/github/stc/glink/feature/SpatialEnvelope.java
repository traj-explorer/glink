package com.github.stc.glink.feature;

import java.io.Serializable;

/**
 * @author Yu Liebing
 * Create on 2020-05-16.
 */
public class SpatialEnvelope implements Serializable {

  private float minLat;
  private float maxLat;
  private float minLng;
  private float maxLng;

  public SpatialEnvelope() { }

  public SpatialEnvelope(float minLat, float maxLat, float minLng, float maxLng) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
  }

  public float getMinLat() {
    return minLat;
  }

  public void setMinLat(float minLat) {
    this.minLat = minLat;
  }

  public float getMaxLat() {
    return maxLat;
  }

  public void setMaxLat(float maxLat) {
    this.maxLat = maxLat;
  }

  public float getMinLng() {
    return minLng;
  }

  public void setMinLng(float minLng) {
    this.minLng = minLng;
  }

  public float getMaxLng() {
    return maxLng;
  }

  public void setMaxLng(float maxLng) {
    this.maxLng = maxLng;
  }

  public <T extends GeoObject> boolean contain(T geoObject) {
    if (geoObject instanceof Point) {
      Point p = (Point) geoObject;
      return p.getLat() > minLat && p.getLat() < maxLat && p.getLng() > minLng && p.getLng() < maxLng;
    }
    return false;
  }
}
