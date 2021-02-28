package com.github.tm.glink.core.types;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class GeometryTypeInfo<T extends Geometry> extends TypeInfo<T> {

  private static final WKTReader wktReader = new WKTReader();

  public static final GeometryTypeInfo<Point> POINT = new GeometryTypeInfo<>();
  public static final GeometryTypeInfo<Polygon> POLYGON = new GeometryTypeInfo<>();

  protected GeometryTypeInfo() { }

  @Override
  public T from(String value) {
    try {
      return (T) wktReader.read(value);
    } catch (ParseException e) {
      e.printStackTrace();
      return null;
    }
  }
}
