package com.github.tm.glink.examples.demo;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

@SuppressWarnings("checkstyle:MethodName")
public class SpatialTypes {

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Point extends ScalarFunction {

    public transient GeometryFactory geometryFactory;

    @Override
    public void open(FunctionContext context) throws Exception {
      geometryFactory = new GeometryFactory();
    }

    public Geometry eval(double x, double y) {
      return geometryFactory.createPoint(new Coordinate(x, y));
    }
  }

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_GeomFromText extends ScalarFunction {

    public transient WKTReader wktReader;

    @Override
    public void open(FunctionContext context) throws Exception {
      wktReader = new WKTReader();
    }

    public Geometry eval(String wkt) throws ParseException {
      return wktReader.read(wkt);
    }
  }

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Contains extends ScalarFunction {
    public boolean eval(Geometry geom1, Geometry geom2) {
      return geom1.contains(geom2);
    }
  }
}
