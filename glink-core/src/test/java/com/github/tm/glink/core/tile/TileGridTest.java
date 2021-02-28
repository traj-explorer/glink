package com.github.tm.glink.core.tile;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.*;

public class TileGridTest {

  TileGrid tileGrid = new TileGrid(2);

  @Test
  public void getPixel() {
    GeometryFactory factory = new GeometryFactory();
    Point point = factory.createPoint(new Coordinate(66.52, 90.1));
    Pixel pixel = tileGrid.getPixel(point);
    System.out.println(pixel);
  }

  @Test
  public void test() {
    int z = 3;
    double lat = 85;
    double lng = 90.1;

    int x = (int) (Math.pow(2, z - 1) * (lng / 180 + 1));
    int y = (int) Math.ceil((Math.pow(2, z - 1) * (1 - Math.log(Math.tan(Math.PI * lat / 180) + 1 / Math.cos(Math.PI * lat / 180)) / Math.PI)));
    System.out.println(x);
    System.out.println(y);
  }


  public static int[] GoogleLonLatToXYZ(double lon, double lat, int zoom) {

    double n = Math.pow(2, zoom);
    double tileX = ((lon + 180) / 360) * n;
    double tileY = (1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n;

    int[] xy = new int[2];

    xy[0] = (int) Math.floor(tileX);
    xy[1] = (int) Math.floor(tileY);

    return xy;
  }

    @Test
  public void test1() {
    int zoom = 3;
    double lat = 79;
    double lon = 90.1;

    double n = Math.pow(2, zoom);
    double tileX = ((lon + 180) / 360) * n;
    double tileY = (1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n;
    System.out.println((int) tileX);
    System.out.println((int) tileY);
  }

  @Test
  public void latLng2Pixel() {
    int zoom = 3;
    double lat = 79;
    double lon = 90.1;

    double n = Math.pow(2, zoom);
    double pixelX = ((lon + 180) / 360) * n * 256 % 256;
    double pixelY = ((1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n) * 256 % 256;
    System.out.println((int) pixelX);
    System.out.println((int) pixelY);
  }
}