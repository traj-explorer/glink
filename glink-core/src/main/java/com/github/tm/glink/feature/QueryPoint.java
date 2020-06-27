package com.github.tm.glink.feature;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public class QueryPoint implements Serializable {

  private double lat;
  private double lng;

  public QueryPoint(double lat, double lng) {
    this.lat = lat;
    this.lng = lng;
  }

  public double getLat() {
    return lat;
  }

  public void setLat(double lat) {
    this.lat = lat;
  }

  public double getLng() {
    return lng;
  }

  public void setLng(double lng) {
    this.lng = lng;
  }
}
