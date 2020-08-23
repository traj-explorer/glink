package com.github.tm.glink.features;

import com.github.tm.glink.features.utils.GeoUtil;
import org.locationtech.jts.geom.Coordinate;

import java.io.Serializable;

/**
 * @author Yu Liebing
 */
public class BoundingBox implements Serializable {

  private double minLat;
  private double maxLat;
  private double minLng;
  private double maxLng;

  public BoundingBox(double minLat, double maxLat, double minLng, double maxLng) {
    this.minLat = minLat;
    this.maxLat = maxLat;
    this.minLng = minLng;
    this.maxLng = maxLng;
  }

  public double getMinLat() {
    return minLat;
  }

  public double getMaxLat() {
    return maxLat;
  }

  public double getMinLng() {
    return minLng;
  }

  public double getMaxLng() {
    return maxLng;
  }

  public BoundingBox getBufferBBox(double distance) {
    Coordinate bottomLeft = new Coordinate(minLat, minLng);
    Coordinate upperLeft = new Coordinate(maxLat, minLng);
    Coordinate upperRight = new Coordinate(maxLat, maxLng);
    Coordinate bottomRight = new Coordinate(minLat, maxLng);

    Coordinate c1, c2;
    double bufferMinLat = minLat, bufferMaxLat = maxLat, bufferMinLng = minLng, bufferMaxLng = maxLng;

    // bottom left
    c1 = GeoUtil.calculateEndingLatLng(bottomLeft, 180, distance);
    bufferMinLat = Math.min(bufferMinLat, c1.getX());
    c2 = GeoUtil.calculateEndingLatLng(bottomLeft, 270, distance);
    bufferMinLng = Math.min(bufferMinLng, c2.getY());

    // upper left
    c1 = GeoUtil.calculateEndingLatLng(upperLeft, 0, distance);
    bufferMaxLat = Math.max(bufferMaxLat, c1.getX());
    c2 = GeoUtil.calculateEndingLatLng(upperLeft, 270, distance);
    bufferMinLng = Math.min(bufferMinLng, c2.getY());

    // upper right
    c1 = GeoUtil.calculateEndingLatLng(upperRight, 0, distance);
    bufferMaxLat = Math.max(bufferMaxLat, c1.getX());
    c2 = GeoUtil.calculateEndingLatLng(upperRight, 90, distance);
    bufferMaxLng = Math.max(bufferMaxLng, c2.getY());

    // bottom right
    c1 = GeoUtil.calculateEndingLatLng(bottomRight, 90, distance);
    bufferMaxLng = Math.max(bufferMaxLng, c1.getY());
    c2 = GeoUtil.calculateEndingLatLng(bottomRight, 180, distance);
    bufferMinLat = Math.min(bufferMinLat, c2.getX());

    return new BoundingBox(bufferMinLat, bufferMaxLat, bufferMinLng, bufferMaxLng);
  }

  public boolean contains(Point p) {
    return p.getLat() > minLat && p.getLat() < maxLat && p.getLng() > minLng && p.getLng() < maxLng;
  }

  @Override
  public String toString() {
    return String.format("BoundingBox{lat=[%f, %f], lng=[%f, %f]}", minLat, maxLat, minLng, maxLng);
  }
}
