package com.github.tm.glink.feature;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * @author Yu Liebing
 */
public class Point extends GeoObject {

  private String id;
  private double lat;
  private double lng;
  private long timestamp;

  public Point() { }

  public Point(double lat, double lng) {
    this.lat = lat;
    this.lng = lng;
  }

  public Point(String id, double lat, double lng, long timestamp) {
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

  @Override
  public Geometry getGeometry(GeometryFactory factory) {
    return factory.createPoint(new Coordinate(lat, lng));
  }
}
