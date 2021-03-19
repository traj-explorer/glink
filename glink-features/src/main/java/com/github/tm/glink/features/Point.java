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
@Deprecated
public class Point extends GeoObject implements TemporalObject {

  private static final int MAX_PRECISION = 10000000;

  // attributes of point
  protected String id;
  protected int lat;
  protected int lng;
  protected long timestamp;

  // index
  protected long index;

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

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public long setTimestamp(long timestamp) {
    return this.timestamp;
  }
  public long getIndex() {
    return index;
  }

  public void setIndex(long index) {
    this.index = index;
  }

  @Override
  public String toString() {
    return String.format("Point{id=%s, lat=%.07f, lng=%.07f, timestamp=%d, index=%d}",
            id, getDoubleLat(), getDoubleLng(), timestamp, index);
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
    return this.id.equals(p.id) && this.lat == p.lat && this.lng == p.lng
            && this.timestamp == p.timestamp && this.index == p.index;
  }

  protected double getDoubleLat() {
    return (double) lat / MAX_PRECISION;
  }

  protected double getDoubleLng() {
    return (double) lng / MAX_PRECISION;
  }

  @SuppressWarnings("checkstyle:LocalVariableName")
  protected int getIntLatLng(double doubleValue) {
    double CARRY = 0.00000005;
    return (int) ((doubleValue + CARRY) * MAX_PRECISION);
  }
}
