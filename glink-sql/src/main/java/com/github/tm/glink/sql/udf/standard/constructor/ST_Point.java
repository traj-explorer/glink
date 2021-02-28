package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

@SuppressWarnings("checkstyle:TypeName")
public class ST_Point extends ScalarFunction {

  private transient GeometryFactory factory;

  @Override
  public void open(FunctionContext context) throws Exception {
    factory = new GeometryFactory();
  }

  public Point eval(double x, double y) {
    return factory.createPoint(new Coordinate(x, y));
  }
}
