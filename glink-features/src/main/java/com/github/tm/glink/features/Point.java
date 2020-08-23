package com.github.tm.glink.features;

import com.github.tm.glink.features.utils.GeoUtil;
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

  @Override
  public Envelope getBufferedEnvelope(double distance) {
    Coordinate start = new Coordinate(lat, lng);
    Coordinate upper = GeoUtil.calculateEndingLatLng(start, 0, distance);
    Coordinate right = GeoUtil.calculateEndingLatLng(start, 90, distance);
    Coordinate bottom = GeoUtil.calculateEndingLatLng(start, 180, distance);
    Coordinate left = GeoUtil.calculateEndingLatLng(start, 270, distance);
    return new Envelope(bottom.getX(), upper.getX(), left.getY(), right.getY());
  }

  public static Point fromString(String line) {
    String[] tokens = line.split(",");
    if (tokens.length == 4) {
      return new Point(tokens[0], Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2]), Long.parseLong(tokens[3]));
    } else if (tokens.length == 5) {
      return new Point(tokens[0], Double.parseDouble(tokens[1]), Double.parseDouble(tokens[2]), Long.parseLong(tokens[3]), Long.parseLong(tokens[4]));
    } else {
      return null;
    }
  }

  @Override
  public int hashCode() {
    return id.hashCode() + Double.hashCode(lat) + Double.hashCode(lng) + Long.hashCode(timestamp);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    // 如果是同一个对象
    if (this == obj) {
      return true;
    }
    // 如果两个对象类型相同
    if (this.getClass() != obj.getClass()) {
      return false;
    }
    Point point = (Point) obj;
    return point.id.equals(id)
        && point.lat == this.lat
        && point.lng == this.lng
        && point.timestamp == this.timestamp;
  }
}
