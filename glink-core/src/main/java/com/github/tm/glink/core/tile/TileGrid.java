package com.github.tm.glink.core.tile;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

/**
 * @author Yu Liebing
 */
public class TileGrid {

  public static final int MAX_LEVEL = 18;

  private int level = MAX_LEVEL;

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
    return getTile(lat, lng, level);
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
    return getPixel(lat, lng, level);
  }

  public static Tile getTile(double lat, double lng, int level) {
    double[] tileXY = getDoubleTileXY(lat, lng, level);
    return new Tile(level, (int) tileXY[0], (int) tileXY[1]);
  }

  public static Pixel getPixel(double lat, double lng, int level) {
    double[] tileXY = getDoubleTileXY(lat, lng, level);
    Tile tile = new Tile(level, (int) tileXY[0], (int) tileXY[1]);
    int pixelX = (int) (tileXY[0] * 256 % 256);
    int pixelY = (int) (tileXY[1] * 256 % 256);
    int pixelNo = pixelX + pixelY * 256;
    return new Pixel(tile, pixelNo);
  }
  // TODO: 现在的瓦片分隔方式太粗暴，后续要改成适配前端的分隔方式。
  private static double[] getDoubleTileXY(double lat, double lng, double level) {
    double n = Math.pow(2, level);
    double tileX = ((lng + 180) / 360) * n;
    double tileY = (1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n;
    return new double[] {tileX, tileY};
  }
}
