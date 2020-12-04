package com.github.tm.glink.core.tile;

import javafx.print.PageOrientation;
import org.junit.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import static org.junit.Assert.*;

public class TileGridTest {

  TileGrid tileGrid = new TileGrid(1);

  @Test
  public void getPixel() {
    GeometryFactory factory = new GeometryFactory();
    Point point = factory.createPoint(new Coordinate(0.15, 0.5));
    Pixel pixel = tileGrid.getPixel(point);
    System.out.println(pixel);
  }
}