package com.github.tm.glink.sql.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/**
 * @author Yu Liebing
 */
public class GeometryConstructor {

  @SuppressWarnings("checkstyle:TypeName")
  public static class ST_Point extends ScalarFunction {

    private transient GeometryFactory factory;

    @Override
    public void open(FunctionContext context) throws Exception {
      factory = new GeometryFactory();
    }

    public Geometry eval(double x, double y) {
      return factory.createPoint(new Coordinate(x, y));
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
  public static class ST_AsText extends ScalarFunction {

    public transient WKTWriter wktWriter;

    @Override
    public void open(FunctionContext context) throws Exception {
      wktWriter = new WKTWriter();
    }

    public String eval(Geometry geom) {
      return wktWriter.write(geom);
    }
  }
}
