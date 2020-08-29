package com.github.tm.glink.features;

import com.github.tm.glink.features.utils.GeoUtil;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

import java.util.Objects;

/**
 * @author Yu Liebing
 */
public class Point extends GeoObject {

  private static int MAX_PRECISION = 10000000;

  // attributes of point
  private String id;
  private int lat;
  private int lng;
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
    this.lat = getIntLatLng(lat);
    this.lng = getIntLatLng(lng);
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
    return getDoubleLat();
  }

  public void setLat(double lat) {
    this.lat = getIntLatLng(lat);
  }

  public double getLng() {
    return getDoubleLng();
  }

  public void setLng(double lng) {
    this.lng = getIntLatLng(lng);
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
//    return String.format("Point{id=%s, lat=%.07f, lng=%.07f, timestamp=%d, index=%d}",
//            id, getDoubleLat(), getDoubleLng(), timestamp, index);
    return id;
  }

  @Override
  public Geometry getGeometry(GeometryFactory factory) {
    return factory.createPoint(new Coordinate(getDoubleLat(), getDoubleLng()));
  }

  @Override
  public Envelope getEnvelope() {
    return new Envelope(getDoubleLat(), getDoubleLat(), getDoubleLng(), getDoubleLng());
  }

  @Override
  public Envelope getBufferedEnvelope(double distance) {
    Coordinate start = new Coordinate(getDoubleLat(), getDoubleLng());
    Coordinate upper = GeoUtil.calculateEndingLatLng(start, 0, distance);
    Coordinate right = GeoUtil.calculateEndingLatLng(start, 90, distance);
    Coordinate bottom = GeoUtil.calculateEndingLatLng(start, 180, distance);
    Coordinate left = GeoUtil.calculateEndingLatLng(start, 270, distance);
    return new Envelope(bottom.getX(), upper.getX(), left.getY(), right.getY());
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, lat, lng, timestamp, index);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof Point)) return false;
    Point p = (Point) o;
    return this.id.equals(p.id) && this.lat == p.lat && this.lng == p.lng &&
            this.timestamp == p.timestamp && this.index == p.index;
  }

  private double getDoubleLat() {
    return (double) lat / MAX_PRECISION;
  }

  private double getDoubleLng() {
    return (double) lng / MAX_PRECISION;
  }
  private int getIntLatLng(double doubleValue) {
    double CARRY = 0.00000005;
    return (int) ((doubleValue + CARRY) * MAX_PRECISION);
  }
}
