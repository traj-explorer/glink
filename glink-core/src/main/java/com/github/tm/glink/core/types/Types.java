package com.github.tm.glink.core.types;

import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;

public class Types {

  public static final TypeInfo<Integer> INTEGER = BasicTypeInfo.INTEGER;
  public static final TypeInfo<Long> LONG = BasicTypeInfo.LONG;
  public static final TypeInfo<Double> DOUBLE = BasicTypeInfo.DOUBLE;

  public static final GeometryTypeInfo<Point> POINT = GeometryTypeInfo.POINT;
  public static final GeometryTypeInfo<Polygon> POLYGON = GeometryTypeInfo.POLYGON;
}
