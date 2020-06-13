package com.github.stc.glink.feature;

/**
 * @author Yu Liebing
 */
public class Point extends GeoObject {

  private String id;
  private float lat;
  private float lng;
  private long timestamp;

  public Point() { }

  public Point(String id, float lat, float lng, long timestamp) {
    this.id = id;
    this.lat = lat;
    this.lng = lng;
    this.timestamp = timestamp;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public float getLat() {
    return lat;
  }

  public void setLat(float lat) {
    this.lat = lat;
  }

  public float getLng() {
    return lng;
  }

  public void setLng(float lng) {
    this.lng = lng;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public String toString() {
    return id + ", " + lat + ", " + lng + ", " + timestamp;
  }
}
