package com.github.tm.glink.sql.udf.standard.constructor;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

public class ST_MakePointM extends ScalarFunction {

  private transient GeometryFactory geometryFactory;

  @Override
  public void open(FunctionContext context) throws Exception {
    geometryFactory = new GeometryFactory();
  }

  public Point eval(double x, double y, double m) {
    return geometryFactory.createPoint(new Coordinate(x, y, m));
  }
}
