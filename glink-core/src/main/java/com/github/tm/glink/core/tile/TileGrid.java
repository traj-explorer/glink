package com.github.tm.glink.core.tile;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class TileGrid {

  public static final int MAX_LEVEL = 18;

  private int level;

  public TileGrid(int level) {
    this.level = level;
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
    return getTile(lat, lng);
  }

  public Pixel getPixel(Geometry geometry) {
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
    return getPixel(lat, lng);
  }

  public Tile getTile(double lat, double lng) {
    double[] tileXY = getTileXYDouble(lat, lng);
    return new Tile(level, (int) tileXY[0], (int) tileXY[1]);
  }

  public Pixel getPixel(double lat, double lng) {
    double[] tileXY = getTileXYDouble(lat, lng);
    Tile tile = getTile(lat, lng);
    int pixelX = (int) (tileXY[0] * 256 % 256);
    int pixelY = (int) (tileXY[1] * 256 % 256);
    int pixelNo = pixelX + pixelY * 256;
    return new Pixel(tile, pixelNo);
  }

  // 使用Web Mercator投影并根据投影后的经纬度获取瓦片行列号。
  // 第一项为瓦片的列号（x），第二项为瓦片的行号（y）
  private double[] getTileXYDouble(double lat, double lng) {
    double n = Math.pow(2, level);
    double  tileX = (lng + 180) / 360 * n;
    double  tileY = ((1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n);
    return new double[] {tileX, tileY};
  }
}
