package com.github.tm.glink.core.tile;

import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

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
    int zoom = 13;
//    double lat = 23.966175871265037;
      double lat = 24.006326198751132;
      double lon = 115.9716796875;

    double n = Math.pow(2, zoom);
    int  tileX = (int)Math.floor((lon + 180)/360 * n);
    int  tileY = (int)((1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n);
    double pixelX = ((lon + 180) / 360) * n * 256 % 256;
    double pixelY = ((1 - (Math.log(Math.tan(Math.toRadians(lat)) + (1 / Math.cos(Math.toRadians(lat)))) / Math.PI)) / 2 * n) * 256 % 256;
    System.out.println("px"  + (int) pixelX);
    System.out.println("py" + (int) pixelY);
    System.out.println("tx" + (int) tileX);
    System.out.println("ty" + (int) tileY);
  }

  @Test
  public void latLng2Pixel() {
    int zoom = 13;
    double lat = 24.006326198751132;
    double lon = 115.9716796875;

    double n = Math.pow(2, zoom);

  }
  @Test
  public void TilePixelToLatLng () {
    int zoom = 13;
    int tileX = 6735;
    int tileY = 3535;
    int pixelX = 0;
    int pixelY = 1;
    double lng = (tileX + pixelX/256.0)/Math.pow(2,zoom) * 360 - 180;
    double lat = Math.atan(Math.sinh(Math.PI - 2 * Math.PI * (tileY + pixelY/256.0) / Math.pow(2, zoom))) * 180 / Math.PI;
    System.out.println(lng);
    System.out.println(lat);
  }
}