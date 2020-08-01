package com.github.tm.glink.fearures;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * @author Yu Liebing
 */
public class Point extends GeoObject {

  // attributes of point
  private String id;
  private double lat;
  private double lng;
  private long timestamp;

  // index
  private long index;

  public Point() { }

  public Point(double lat, double lng) {
    this(null, lat, lng, 0L, 0L);
  }

  public Point(String id, double lat, double lng, long timestamp) {
    this(id, lat, lng, timestamp, 0L);
  }

  public Point(String id, double lat, double lng, long timestamp, long index) {
    this.id = id;
    this.lat = lat;
    this.lng = lng;
    this.timestamp = timestamp;
    this.index = index;
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

  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  @Override
  public String toString() {
    return String.format("Point{id=%s, lat=%f, lng=%f, timestamp=%d, index=%d}", id, lat, lng, timestamp, index);
  }

  @Override
  public Geometry getGeometry(GeometryFactory factory) {
    return factory.createPoint(new Coordinate(lat, lng));
  }

  @Override
  public Envelope getEnvelope() {
    return new Envelope(lat, lat, lng, lng);
  }
}
