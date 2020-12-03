package com.github.tm.glink.core.tile;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class TileGrid {

  public static final int MAX_LEVEL = 23;
  public static final double MIN_LAT = -85.05112877980659;
  public static final double MIN_LNG = -180.;
  public static final double MAX_LAT = 85.05112877980659;
  public static final double MAX_LNG = 180.;

  private double minLat;
  private double minLng;
  private double maxLat;
  private double maxLng;

  private int level = MAX_LEVEL;

  private double latStep;
  private double lngStep;

  public TileGrid(double minLat, double minLng, double maxLat, double maxLng, int level) {
    this.minLat = minLat;
    this.minLng = minLng;
    this.maxLat = maxLat;
    this.maxLng = maxLng;
    this.level = level;
    init();
  }

  public TileGrid(int level) {
    this(MIN_LAT, MIN_LNG, MAX_LAT, MAX_LNG, level);
  }

  public TileGrid() {
    this(MIN_LAT, MIN_LNG, MAX_LAT, MAX_LNG, MAX_LEVEL);
  }

  private void init() {
    double split = Math.pow(2, level);
    latStep = (maxLat - minLat) / split;
    lngStep = (maxLng - minLng) / split;
  }

  public Tile getTile(Geometry geometry) {
    double lat;
    double lng;
    if (geometry instanceof Point) {
      Point p = (Point) geometry;
      lat = p.getX();
      lng = p.getY();
    } else {  // TODO: how to get tile for polygon?
      Point centroid = geometry.getCentroid();
      lat = centroid.getX();
      lng = centroid.getY();
    }
    int x = (int) ((lng - minLng) / lngStep);
    int y = (int) ((maxLat - lat) / latStep);
    return new Tile(level, x, y);
  }

  public Pixel getPixel(Geometry geometry) {
    Tile tile = getTile(geometry);
    double tileMinLng = tile.getX() * lngStep - maxLng;
    double tileMaxLng = (tile.getX() + 1) * lngStep - maxLng;
    double tileMinLat = maxLat - (tile.getY() + 1) * latStep;
    double tileMaxLat = maxLat - tile.getY() * latStep;

    double tileLngStep = (tileMaxLng - tileMinLng) / 256;
    double tileLatStep = (tileMaxLat - tileMinLat) / 256;

    double lat;
    double lng;
    if (geometry instanceof Point) {
      Point p = (Point) geometry;
      lat = p.getX();
      lng = p.getY();
    } else {  // TODO: how to get tile for polygon?
      Point centroid = geometry.getCentroid();
      lat = centroid.getX();
      lng = centroid.getY();
    }

    int pixelX = (int) ((lng - tileMinLng) / tileLngStep);
    int pixelY = (int) ((tileMaxLat - lat) / tileLatStep);
    int pixelNo = pixelX * 256 + pixelY;

    return new Pixel(tile, pixelNo);
  }
}
